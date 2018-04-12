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

package com.spotify.scio.avro

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.{Pipeline, PipelineResult, io => gio}
import org.apache.beam.sdk.transforms.DoFn
import com.google.protobuf.Message
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import com.spotify.scio.ScioContext
import com.spotify.scio.Implicits._
import com.spotify.scio.values._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.testing._
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final class AvroScioContext(@transient val self: ScioContext) extends Serializable {

  import self.{requireNotClosed, pipeline, wrap, options}

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group input
   */
  def objectFile[T: ClassTag](path: String): SCollection[T] = requireNotClosed {
    if (self.isTest) {
      self.getTestInput(ObjectFileIO[T](path))
    } else {
      val coder = pipeline.getCoderRegistry.getScalaCoder[T](options)
      this.avroFile[GenericRecord](path, AvroBytesUtil.schema)
        .parDo(new DoFn[GenericRecord, T] {
          @ProcessElement
          private[scio] def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit = {
            c.output(AvroBytesUtil.decode(coder, c.element()))
          }
        })
        .setName(path)
    }
  }

  /**
   * Get an SCollection for an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group input
   */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): SCollection[T] =
  requireNotClosed {
    if (self.isTest) {
      self.getTestInput(AvroIO[T](path))
    } else {
      val cls = ScioUtil.classOf[T]
      val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        gio.AvroIO.read(cls).from(path)
      } else {
        gio.AvroIO.readGenericRecords(schema).from(path).asInstanceOf[gio.AvroIO.Read[T]]
      }
      wrap(self.applyInternal(t)).setName(path)
    }
  }

  /**
    * Get a typed SCollection from an Avro schema.
    *
    * Note that `T` must be annotated with
    * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
    * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
    * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
    *
    * @group input
    */
  def typedAvroFile[T <: HasAvroAnnotation : ClassTag : TypeTag](path: String)
  : SCollection[T] = requireNotClosed {
    if (self.isTest) {
      self.getTestInput(AvroIO[T](path))
    } else {
      val avroT = AvroType[T]
      val t = gio.AvroIO.readGenericRecords(avroT.schema).from(path)
      wrap(self.applyInternal(t)).setName(path).map(avroT.fromGenericRecord)
    }
  }

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group input
   */
  def protobufFile[T: ClassTag](path: String)(implicit ev: T <:< Message): SCollection[T] =
    requireNotClosed {
      if (self.isTest) {
        self.getTestInput(ProtobufIO[T](path))
      } else {
        objectFile(path)
      }
    }
}