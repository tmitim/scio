/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.jmh

import java.io.{InputStream, OutputStream}
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.spotify.scio.coders._
import com.twitter.chill.IKryoRegistrar
import org.apache.beam.sdk.coders.{Coder, AtomicCoder, ByteArrayCoder, SerializableCoder, StringUtf8Coder, VarIntCoder}
import org.apache.beam.sdk.util.CoderUtils
import org.openjdk.jmh.annotations._

final case class UserId(bytes: Array[Byte])
final case class User(id: UserId, username: String, email: String)
final case class SpecializedUser(id: UserId, username: String, email: String)
final case class SpecializedUserForDerived(id: UserId, username: String, email: String)

trait LowPriorityCoderDerivation {
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

  // TODO: test that this actually works
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

  implicit def gen[T]: Coder[T] = macro Magnolia.gen[T]
}

object Implicits extends LowPriorityCoderDerivation {
  implicit def byteArrayCoder: Coder[Array[Byte]] = ByteArrayCoder.of()
  implicit def stringCoder: Coder[String] = StringUtf8Coder.of()

  def SCoder[T](implicit c: Coder[T]): Coder[T] = c
}

import Implicits._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class KryoAtomicCoderBenchmark {

  // please don't use arrays outside of benchmarks
  val userId = UserId(Array[Byte](1, 2, 3, 4))

  // use standard coders
  val user = User(userId, "johndoe", "johndoe@spotify.com")

  // use hand-optimized coders
  val specializedUser = SpecializedUser(userId, "johndoe", "johndoe@spotify.com")
  val specializedUserForDerived = SpecializedUserForDerived(userId, "johndoe", "johndoe@spotify.com")
  val tenTimes = List.fill(10)(specializedUserForDerived)

  val kryoCoder = new KryoAtomicCoder[User](KryoOptions())
  val javaCoder = SerializableCoder.of(classOf[User])
  val specializedCoder = new SpecializedCoder
  val specializedKryoCoder = new KryoAtomicCoder[SpecializedUser](KryoOptions())
  val derivedCoder = SCoder[SpecializedUserForDerived]
  val derivedListCoder = SCoder[List[SpecializedUserForDerived]]

  @Benchmark
  def kryoEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(kryoCoder, user)
  }

  @Benchmark
  def javaEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(javaCoder, user)
  }

  @Benchmark
  def customEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(specializedCoder, specializedUser)
  }

  @Benchmark
  def customKryoEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(specializedKryoCoder, specializedUser)
  }

  @Benchmark
  def derivedEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(derivedCoder, specializedUserForDerived)
  }

  @Benchmark
  def derivedListEncode: Array[Byte] = {
    CoderUtils.encodeToByteArray(derivedListCoder, tenTimes)
  }


  val kryoEncoded = kryoEncode
  val javaEncoded = javaEncode
  val customEncoded = customEncode
  val customKryoEncoded = customKryoEncode
  val derivedEncoded = derivedEncode
  val derivedListEncoded = derivedListEncode

  @Benchmark
  def kryoDecode: User = {
    CoderUtils.decodeFromByteArray(kryoCoder, kryoEncoded)
  }

  @Benchmark
  def javaDecode: User = {
    CoderUtils.decodeFromByteArray(javaCoder, javaEncoded)
  }

  @Benchmark
  def customDecode: SpecializedUser = {
    CoderUtils.decodeFromByteArray(specializedCoder, customEncoded)
  }

  @Benchmark
  def customKryoDecode: SpecializedUser = {
    CoderUtils.decodeFromByteArray(specializedKryoCoder, customKryoEncoded)
  }

  @Benchmark
  def derivedDecode: SpecializedUserForDerived = {
    CoderUtils.decodeFromByteArray(derivedCoder, derivedEncoded)
  }

  @Benchmark
  def derivedListDecode: List[SpecializedUserForDerived] = {
    CoderUtils.decodeFromByteArray(derivedListCoder, derivedListEncoded)
  }

  // assert(derivedDecode == specializedUserForDerived, s"$derivedDecode != $specializedUserForDerived")
  // assert(derivedListDecode == tenTimes, s"$derivedListDecode != $tenTimes")
}

final class SpecializedCoder extends AtomicCoder[SpecializedUser] {
  def encode(value: SpecializedUser, os: OutputStream): Unit = {
    ByteArrayCoder.of().encode(value.id.bytes, os)
    StringUtf8Coder.of().encode(value.username, os)
    StringUtf8Coder.of().encode(value.email, os)
  }

  def decode(is: InputStream): SpecializedUser = {
    SpecializedUser(
      UserId(ByteArrayCoder.of().decode(is)),
      StringUtf8Coder.of().decode(is),
      StringUtf8Coder.of().decode(is)
    )
  }
}

final class SpecializedKryoSerializer extends Serializer[SpecializedUser] {
  def read(kryo: Kryo, input: Input, tpe: Class[SpecializedUser]): SpecializedUser = {
    val len = input.readInt()
    val array = new Array[Byte](len)

    input.readBytes(array)

    val username = input.readString()
    val email = input.readString()

    SpecializedUser(UserId(array), username, email)
  }

  def write(kryo: Kryo, output: Output, obj: SpecializedUser): Unit = {
    output.writeInt(obj.id.bytes.length)
    output.writeBytes(obj.id.bytes)
    output.writeString(obj.username)
    output.writeString(obj.email)
  }
}

@KryoRegistrar
class KryoRegistrar extends IKryoRegistrar {
  def apply(k: Kryo): Unit = {
    k.register(classOf[User])
    k.register(classOf[SpecializedUser], new SpecializedKryoSerializer)
    k.register(classOf[UserId])
    k.register(classOf[Array[Byte]])

    k.setRegistrationRequired(true)
  }
}
