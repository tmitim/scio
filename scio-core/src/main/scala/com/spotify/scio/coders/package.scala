/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders

import java.io.{InputStream, OutputStream}
import scala.annotation.implicitNotFound
import org.apache.beam.sdk.coders.{CustomCoder, Coder => BCoder, KvCoder}
import com.spotify.scio.ScioContext
import scala.reflect.ClassTag

@implicitNotFound("""
Cannot find a Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have a Coder instance in scope. Here are
  some debugging hints:
    - Make sure you imported com.spotify.scio.coders.Implicits._
    - For case classes or sealed traits, annotate the definition with @deriveCoder:
        @deriveCoder
        final case class User(id: Long, username: String, email: String)
    - If you can't annotate the definition, you can also generate a Coder using
        implicit val someClassCoder = com.spotify.scio.coders.Implicits.gen[SomeCaseClass]
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - You can check that an instance exists for Coder in the REPL or in your code:
        scala> Coder[Foo]
    And find the missing instance and construct it as needed.
""")
sealed trait Coder[T]
final case class Beam[T] private (beam: BCoder[T]) extends Coder[T]
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T]
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B]

final case class XMapCoder[A, B](bc: BCoder[A], f: A => B, t: B => A) extends CustomCoder[B] {
  def encode(value: B, os: OutputStream): Unit =
    bc.encode(t(value), os)
  def decode(is: InputStream): B =
    f(bc.decode(is))
}

trait CoderGrammar {
  import org.apache.beam.sdk.coders.CoderRegistry
  import org.apache.beam.sdk.options.PipelineOptions
  import org.apache.beam.sdk.options.PipelineOptionsFactory

  def beam[T](beam: BCoder[T]) =
    Beam(beam)
  def fallback[T](implicit ct: ClassTag[T]) =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]) =
    Transform(c, f)
  def xmap[A, B](c: Coder[A])(f: A => B, t: B => A): Coder[B] =
    Transform[A, B](c, bc => beam(XMapCoder(bc, f, t)))

  def beam[T](sc: ScioContext, c: Coder[T]): BCoder[T] =
    beam(sc.pipeline.getCoderRegistry, sc.options, c)

  def beamWithDefault[T](
    coder: Coder[T],
    r: CoderRegistry = CoderRegistry.createDefault(),
    o: PipelineOptions = PipelineOptionsFactory.create()) =
      beam(r, o, coder)

  def beam[T](r: CoderRegistry, o: PipelineOptions, c: Coder[T]): BCoder[T] = {
    c match {
      case Beam(c) => c
      case Fallback(ct) =>
        com.spotify.scio.Implicits.RichCoderRegistry(r)
          .getScalaCoder[T](o)(ct)
      case Transform(c, f) =>
        val u = f(beam(r, o, c))
        beam(r, o, u)
    }
  }
}

object Coder extends CoderGrammar with AtomCoders with TupleCoders {
  // TODO: better error message
  @deprecated("""
    No implicit coder found for type ${T}. Using a fallback coder.
    Most types should be supported out of the box by simply importing `com.spotify.scio.coders.Implicits._`.
    If a type is not supported, consider implementing your own implicit com.spotify.scio.coders.Coder for this type:

      class MyTypeCoder extends Coder[MyType] {
        def decode(in: InputStream): MyType = ???
        def encode(ts: MyType, out: OutputStream): Unit = ???
      }
      implicit def myTypeCoder: Coder[MyType] =
        new MyTypeCoder
    """, since="0.6.0")
  implicit def implicitFallback[T: ClassTag](implicit lp: shapeless.LowPriority): Coder[T] =
    Coder.fallback[T]

  import org.apache.beam.sdk.values.KV
  def kvCoder[K, V](ctx: ScioContext)(implicit k: Coder[K], v: Coder[V]): KvCoder[K, V] =
    KvCoder.of(Coder.beam(ctx, Coder[K]), Coder.beam(ctx, Coder[V]))

  def apply[T](implicit c: Coder[T]): Coder[T] = c

  def clean[T](w: Coder[T]) =
    com.spotify.scio.util.ClosureCleaner.clean(w).asInstanceOf[Coder[T]]
}

import scala.language.experimental.macros
object Macros {
  import scala.reflect.macros._

  def addImplicitCoderDerivation(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    annottees.map(_.tree) match {
      case (clazzDef @ q"$mods class $cName[..$tparams] $ctorMods(..$fields) extends { ..$earlydefns } with ..$parents { $self => ..$body }") :: tail if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        val maybeCompanion = tail.headOption
        val tree =
          q"""
            $clazzDef
            ${companion(c)(cName, maybeCompanion, tparams)}
          """
        c.Expr[Any](tree)
      case (clazzDef @ q"$mods trait $cName[..$tparams] extends { ..$earlydefns } with ..$parents { $self => ..$body }") :: tail if mods.asInstanceOf[Modifiers].hasFlag(Flag.SEALED) =>
        val maybeCompanion = tail.headOption
        val tree =
          q"""
            $clazzDef
            ${companion(c)(cName, maybeCompanion, tparams)}
          """
        c.Expr[Any](tree)
      case t =>
        c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }
  }

  // TODO: check if companion already contains a coder definition
  private def companion(c: blackbox.Context)
                       (name: c.TypeName, originalCompanion: Option[c.Tree], typeParams: List[c.universe.TypeDef]): c.Tree = {
    import c.universe._

    val tyn = TypeName(name.toString)
    val tn = TermName(name.toString)
    val coderName = TermName("coder" + name.toString)
    val typeParamsNames = typeParams.map(_.name)
    val imps =
      typeParamsNames.zipWithIndex.map { case (t, i) =>
        val pn = TermName("coder" + i)
        q"""$pn: _root_.com.spotify.scio.coders.Coder[$t]"""
      }

    val m =
      q"""
        implicit def $coderName[..$typeParams](implicit ..$imps): _root_.com.spotify.scio.coders.Coder[$tyn[..$typeParamsNames]] =
          _root_.com.spotify.scio.coders.Implicits.gen[$tyn[..$typeParamsNames]]
      """
    val tree =
      originalCompanion.map { o =>
        val q"$mods object $cName extends { ..$_ } with ..$parents { $_ => ..$body }" = o
        // need to filter out Object, otherwise get duplicate Object error
        // also can't get a FQN of scala.AnyRef which gets erased to java.lang.Object, can't find a
        // sane way to =:= scala.AnyRef
        val filteredTraits = parents.toSet.filterNot(_.toString == "scala.AnyRef")
        q"""$mods object $cName extends ..$filteredTraits {
            ..${body :+ m}
          }"""
      }.getOrElse{ q"object $tn { ..$m }" }
    tree
  }
}

class deriveCoder extends scala.annotation.StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro Macros.addImplicitCoderDerivation
}