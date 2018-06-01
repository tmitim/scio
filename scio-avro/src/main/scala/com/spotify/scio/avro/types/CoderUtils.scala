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

private[scio] abstract class WrappedCoder[T] extends AtomicCoder[T] {
  val underlying: Coder[T]

  def encode(value: T, os: OutputStream): Unit =
    underlying.encode(value, os)
  def decode(is: InputStream): T =
    underlying.decode(is)
}

private[scio] object CoderUtils {

  def staticInvokeCoder[T <: SpecificRecordBase : c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val companionSymbol = companioned.companion
    val companionType = companionSymbol.typeSignature
    q"""
    _root_.com.spotify.scio.coders.MkCoder[$companioned] {
      (value: $companioned, os: _root_.java.io.OutputStream) =>
        os.write(value.toByteBuffer().array())
      }{ is =>
        val bytes = java.nio.ByteBuffer.wrap(org.apache.commons.io.IOUtils.toByteArray(is))
        ${companionType}.fromByteBuffer(bytes)
      }
    """
  }

  def wrappedCoder[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    q"""
    new _root_.com.spotify.scio.avro.types.WrappedCoder[$wtt] {
      val underlying: com.spotify.scio.coders.Coder[$wtt] = ${magnolia.Magnolia.gen[T](c)}
    }
    """
  }

}
