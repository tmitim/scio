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
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.util.CoderUtils
import com.twitter.bijection._

trait FromBijection {

  implicit def collectionfromBijection[A, B](
    implicit b: Bijection[Seq[A], B], //TODO: should I use ImplicitBijection ?
             c: Coder[Seq[A]]): Coder[B] =
    new AtomicCoder[B] {
      def decode(in: InputStream): B = b(c.decode(in))
      def encode(ts: B, out: OutputStream): Unit = c.encode(b.invert(ts), out)
    }

  implicit def mapfromBijection[K, A, B](
    implicit b: Bijection[Map[K, A], B], //TODO: should I use ImplicitBijection ?
             c: Coder[Map[K, A]]): Coder[B] =
    new AtomicCoder[B] {
      def decode(in: InputStream): B = b(c.decode(in))
      def encode(ts: B, out: OutputStream): Unit = c.encode(b.invert(ts), out)
    }

}

trait LowPriorityCoderDerivation extends FromBijection {
  import language.experimental.macros, magnolia._

  type Typeclass[T] = Coder[T]

  def combine[T](ctx: CaseClass[Coder, T]): Coder[T] =
    new AtomicCoder[T] {
      def encode(value: T, os: OutputStream): Unit =
        ctx.parameters.foreach { p =>
          p.typeclass.encode(p.dereference(value), os)
        }

      def decode(is: InputStream): T = {
        ctx.construct { p => p.typeclass.decode(is) }
      }
    }

  def dispatch[T](sealedTrait: SealedTrait[Coder, T]): Coder[T] =
    new AtomicCoder[T] {
      val idx: Map[TypeName, Int] = sealedTrait.subtypes.map(_.typeName).zipWithIndex.toMap
      val idc = VarIntCoder.of()

      def encode(value: T, os: OutputStream): Unit =
        sealedTrait.dispatch(value) { subtype =>
          idc.encode(idx(subtype.typeName), os)
          subtype.typeclass.encode(subtype.cast(value), os)
        }

      def decode(is: InputStream): T = {
        val id = idc.decode(is)
        val subtype = sealedTrait.subtypes(id)
        subtype.typeclass.decode(is)
      }
    }

  // TODO: can we provide magnolia nice error message when gen is used implicitly ?
  implicit def gen[T]: Coder[T] = macro Magnolia.gen[T]

  import org.apache.avro.specific.SpecificRecordBase
  import com.spotify.scio.avro.types.CoderUtils
  implicit def genAvro[T <: SpecificRecordBase]: Coder[T] =
    macro CoderUtils.staticInvokeCoder[T]
}

object fallback {
  import scala.reflect.ClassTag
  @deprecated("""
  Coders in `com.spotify.scio.coders.fallback._` are very slow and unsafe.
  They are only provided for compatibility reasons.
  Most types should be supported out of the box by simply importing `com.spotify.scio.coders.Implicits._`.
  If a type is not supported, consider implementing your own implicit Coder for this type:

    implicit def myTypeCoder: Coder[MyType] =
      new AtomicCoder[MyType] {
        def decode(in: InputStream): MyType = ???
        def encode(ts: MyType, out: OutputStream): Unit = ???
      }
  """, since="0.6.0")
  private[scio] def apply[V: ClassTag](p: com.spotify.scio.values.SCollection[_]): Coder[V] =
    p.getCoder[V]
}

object Implicits extends LowPriorityCoderDerivation {

  // TODO: support all primitive types
  // BigDecimalCoder
  // BigIntegerCoder
  // BitSetCoder
  // BooleanCoder
  // ByteStringCoder

  // DurationCoder
  // InstantCoder

  // TableRowJsonCoder
  implicit def byteCoder: Coder[Byte] = ByteCoder.of().asInstanceOf[Coder[Byte]]
  implicit def byteArrayCoder: Coder[Array[Byte]] = ByteArrayCoder.of()
  implicit def stringCoder: Coder[String] = StringUtf8Coder.of()
  implicit def intCoder: Coder[Int] = VarIntCoder.of().asInstanceOf[Coder[Int]]
  implicit def doubleCoder: Coder[Double] = DoubleCoder.of().asInstanceOf[Coder[Double]]

  def genericRecordCoder(schema: org.apache.avro.Schema) = AvroCoder.of(schema)

  implicit def seqCoder[T: Coder]: Coder[Seq[T]] =
    new AtomicCoder[Seq[T]] {
      val lc = VarIntCoder.of()
      def decode(in: InputStream): Seq[T] = {
        val l = lc.decode(in)
        (1 to l).map { _ =>
          Coder[T].decode(in)
        }
      }

      def encode(ts: Seq[T], out: OutputStream): Unit = {
        lc.encode(ts.length, out)
        ts.foreach { v => Coder[T].encode(v, out) }
      }
    }

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    new AtomicCoder[Map[K, V]] {
      val lc = VarIntCoder.of()
      def decode(in: InputStream): Map[K, V] = {
        val l = lc.decode(in)
        (1 to l).map { _ =>
          val k = Coder[K].decode(in)
          val v = Coder[V].decode(in)
          (k, v)
        }.toMap
      }

      def encode(ts: Map[K, V], out: OutputStream): Unit = {
        lc.encode(ts.size, out)
        ts.foreach { case (k, v) =>
          Coder[K].encode(k, out)
          Coder[V].encode(v, out)
        }
      }
    }
}