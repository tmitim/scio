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

// Avoid having to explicitly import beam coder everywhere
// type Coder[T] = org.apache.beam.sdk.coders.Coder[T]

@implicitNotFound("""
Cannot find or construct a Coder instance for type:

  ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have a Coder instance in scope. Here are
  some debugging hints:
    - Make sure you imported com.spotify.scio.coders.Implicits._
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - For case classes ensure that each element has a Coder instance in scope.
      You can get a detailed explanation by calling the automatic derivation explicitly:
      scala> com.spotify.scio.coders.Implicits.gen[Foo]
      <console>:18: error: magnolia: could not find Coder.Typeclass for type com.spotify.scio.WeirdType
          in parameter 'x' of product type Foo
    - For Tuples, use the same method as for case classes.
    You can check that an instance exists for Coder in the REPL or in your code:
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