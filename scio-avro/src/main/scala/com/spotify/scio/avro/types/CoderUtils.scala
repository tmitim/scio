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

package com.spotify.scio.avro.types

import org.apache.avro.specific.SpecificRecordBase
import scala.reflect.macros._

import org.apache.beam.sdk.coders.{Coder, AtomicCoder}
import java.io.{InputStream, OutputStream}

trait WrappedCoder[T] extends AtomicCoder[T] with Serializable {
  def underlying: Coder[T] = ???
  def encode(value: T, os: OutputStream): Unit =
    underlying.encode(value, os)
  def decode(is: InputStream): T =
    underlying.decode(is)
}

final class AvroRawCoder[T](@transient var schema: org.apache.avro.Schema) extends AtomicCoder[T] {

  // makes the schema scerializable
  private val schemaString = schema.toString

  if(schema == null) {
    schema = new org.apache.avro.Schema.Parser().parse(schemaString)
  }

  @transient lazy val model = new org.apache.avro.specific.SpecificData()
  @transient lazy val encoder = new org.apache.avro.message.RawMessageEncoder[T](model, schema)
  @transient lazy val decoder = new org.apache.avro.message.RawMessageDecoder[T](model, schema)

  def encode(value: T, os: OutputStream): Unit =
    encoder.encode(value, os)

  def decode(is: InputStream): T =
    decoder.decode(is)
}

private[scio] object CoderUtils {


  /**
  * Generate a coder which does not serializa the schema and relies exclusively on type.
  */
  def staticInvokeCoder[T <: SpecificRecordBase : c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val companionSymbol = companioned.companion
    val companionType = companionSymbol.typeSignature

    q"""
    new _root_.com.spotify.scio.avro.types.AvroRawCoder[$companioned](${companionType}.getClassSchema())
    """
  }

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedCoder[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val magTree = magnolia.Magnolia.gen[T](c)

    def getLazyVal =
        magTree match {
          case q"lazy val $name = $body; $rest" =>
            body
        }

    val name = c.freshName(s"DerivedCoder")
    val className = TypeName(name)
    val termName = TermName(name)

    //TODO: customize serialization to only keep underlying and get rid of $outer references
    val tree: c.Tree =
      q"""{
      class $className extends _root_.org.apache.beam.sdk.coders.AtomicCoder[$wtt] with java.io.Externalizable {
        var underlying: com.spotify.scio.coders.Coder[$wtt] = $getLazyVal
        def encode(value: $wtt, os: java.io.OutputStream): Unit =
          underlying.encode(value, os)
        def decode(is: java.io.InputStream): $wtt =
          underlying.decode(is)
        override def writeExternal(oss: _root_.java.io.ObjectOutput): Unit = {
          oss.writeObject(underlying)
        }
        override def readExternal(ois: _root_.java.io.ObjectInput): Unit = {
          underlying = ois.readObject().asInstanceOf[Coder[$wtt]]
        }
      }
      new $className
      }"""
    // println(tree)
    tree
  }

}
