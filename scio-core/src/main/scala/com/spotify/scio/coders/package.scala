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
import org.apache.beam.sdk.coders.{AtomicCoder, Coder => BCoder, KvCoder}

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
trait Coder[T] extends Serializable {
  self =>
  def decode(in: InputStream): T
  def encode(ts: T, out: OutputStream): Unit

  def toBeam: BCoder[T] =
    new AtomicCoder[T] {
      def decode(in: InputStream): T = self.decode(in)
      def encode(ts: T, out: OutputStream): Unit = self.encode(ts, out)
    }

  // def xmap[A](f: A => T, t: T => A): Coder[A] =
  //   new Coder[A] {
  //     def decode(in: InputStream): A = t(self.decode(in))
  //     def encode(ts: A, out: OutputStream): Unit = self.encode(f(ts), out)
  //   }
}

class FromBCoder[T](a: BCoder[T]) extends Coder[T] {
  def decode(in: InputStream): T = a.decode(in)
  def encode(ts: T, out: OutputStream): Unit = a.encode(ts, out)
}

object Coder extends AtomCoders with TupleCoders {
  def from[T](a: BCoder[T]): Coder[T] = new FromBCoder(a)
  def apply[T](implicit c: Coder[T]): Coder[T] = c
  def clean[T](w: WrappedCoder[T]) =
    com.spotify.scio.util.ClosureCleaner.clean(w).asInstanceOf[WrappedCoder[T]]
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