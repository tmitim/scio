/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.avro.nio

import com.spotify.scio.ScioContext
import com.spotify.scio.values._
import com.spotify.scio.io.Tap
import com.spotify.scio.nio.ScioIO

import org.apache.avro.Schema
import scala.reflect.ClassTag
import scala.concurrent.Future

case class File[T: ClassTag](path: String, schema: Schema = null)
  extends ScioIO[T] {

  def id: String = ???
  def read(sc: ScioContext, params: ReadP): SCollection[T] = ???
  def tap(read: ReadP): Tap[T] = ???
  def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = ???
}