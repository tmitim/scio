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

import scala.concurrent.Future
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import com.google.protobuf.Message
import org.apache.beam.sdk.{Pipeline, PipelineResult, io => gio}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import com.spotify.scio._
import com.spotify.scio.io._
import com.spotify.scio.avro.io._
import com.spotify.scio.util._
import com.spotify.scio.values._
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[SCollection]] with Avro methods. */
final class AvroSCollection[T](@transient val self: SCollection[T]) extends Serializable {

  import self.{context, pathWithShards, saveAsInMemoryTap, ct}

  /**
   * Extract data from this SCollection as a `Future`. The `Future` will be completed once the
   * pipeline completes successfully.
   * @group output
   */
  def materialize: Future[Tap[T]] = materialize(ScioUtil.getTempFile(context), isCheckpoint = false)
  private[scio] def materialize(path: String, isCheckpoint: Boolean): Future[Tap[T]] =
    internalSaveAsObjectFile(path, isCheckpoint = isCheckpoint, isMaterialized = true)

  private def avroOut[U](write: gio.AvroIO.Write[U],
                         path: String, numShards: Int, suffix: String,
                         codec: CodecFactory,
                         metadata: Map[String, AnyRef]) =
    write
      .to(pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix + ".avro")
      .withCodec(codec)
      .withMetadata(metadata.asJava)

  private def typedAvroOut[U](write: gio.AvroIO.TypedWrite[U, Void, GenericRecord],
                              path: String, numShards: Int, suffix: String,
                              codec: CodecFactory,
                              metadata: Map[String, AnyRef]) =
    write
      .to(pathWithShards(path))
      .withNumShards(numShards)
      .withSuffix(suffix + ".avro")
      .withCodec(codec)
      .withMetadata(metadata.asJava)

   /**
   * Save this SCollection as an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group output
   */
  def saveAsAvroFile(path: String,
                     numShards: Int = 0,
                     schema: Schema = null,
                     suffix: String = "",
                     codec: CodecFactory = CodecFactory.deflateCodec(6),
                     metadata: Map[String, AnyRef] = Map.empty)
  : Future[Tap[T]] =
    if (context.isTest) {
      ???
      // context.testOut(AvroIO(path))(this)
      // saveAsInMemoryTap
    } else {
      val cls = ScioUtil.classOf[T]
      val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        gio.AvroIO.write(cls)
      } else {
        gio.AvroIO.writeGenericRecords(schema).asInstanceOf[gio.AvroIO.Write[T]]
      }
      self.applyInternal(avroOut(t, path, numShards, suffix, codec, metadata))
      context.makeFuture(AvroTap(ScioUtil.addPartSuffix(path), schema))
    }

  /**
    * Save this SCollection as an Avro file. Note that element type `T` must be a case class
    * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
    * @group output
    */
  def saveAsTypedAvroFile(path: String,
                          numShards: Int = 0,
                          suffix: String = "",
                          codec: CodecFactory = CodecFactory.deflateCodec(6),
                          metadata: Map[String, AnyRef] = Map.empty)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAvroAnnotation)
  : Future[Tap[T]] = {
    val avroT = types.AvroType[T]
    if (context.isTest) {
      // context.testOut(AvroIO(path))(this)
      // saveAsInMemoryTap
      ???
    } else {
      val t = gio.AvroIO.writeCustomTypeToGenericRecords()
        .withFormatFunction(new SerializableFunction[T, GenericRecord] {
          override def apply(input: T): GenericRecord = avroT.toGenericRecord(input)
        })
        .withSchema(avroT.schema)
      self.applyInternal(typedAvroOut(t, path, numShards, suffix, codec, metadata))
      context.makeFuture(AvroTap(ScioUtil.addPartSuffix(path), avroT.schema))
    }
  }

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group output
   */
  def saveAsObjectFile(path: String, numShards: Int = 0, suffix: String = ".obj",
                       metadata: Map[String, AnyRef] = Map.empty): Future[Tap[T]] =
    internalSaveAsObjectFile(path, numShards, suffix, metadata)

  private def internalSaveAsObjectFile(path: String, numShards: Int = 0, suffix: String = ".obj",
                                       metadata: Map[String, AnyRef] = Map.empty,
                                       isCheckpoint: Boolean = false,
                                       isMaterialized: Boolean = false)
  : Future[Tap[T]] = {
    if (context.isTest) {
      // (isCheckpoint, isMaterialized) match {
      //   // if it's a test and checkpoint - no need to test checkpoint data
      //   case (true, _) => ()
      //   // Do not run assertions on materilized value but still access test context to trigger
      //   // the test checking if we're running inside a JobTest
      //   case (_, true) => context.testOutNio
      //   case _ => context.testOut(ObjectFileIO(path))(this)
      // }
      // saveAsInMemoryTap
      ???
    } else {
      val elemCoder = self.getCoder[T]
      self
        .parDo(new DoFn[T, GenericRecord] {
          @ProcessElement
          private[scio] def processElement(c: DoFn[T, GenericRecord]#ProcessContext): Unit =
            c.output(AvroBytesUtil.encode(elemCoder, c.element()))
        })
        .saveAsAvroFile(path, numShards, AvroBytesUtil.schema, suffix, metadata = metadata)
      context.makeFuture(ObjectFileTap[T](ScioUtil.addPartSuffix(path)))
    }
  }


  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group output
   */
  def saveAsProtobufFile(path: String, numShards: Int = 0)
                        (implicit ev: T <:< Message): Future[Tap[T]] = {
    if (context.isTest) {
      // context.testOut(ProtobufIO(path))(this)
      // saveAsInMemoryTap
      ???
    } else {
      import me.lyh.protobuf.generic
      val schema = generic.Schema.of[Message](ct.asInstanceOf[ClassTag[Message]]).toJson
      val metadata = Map("protobuf.generic.schema" -> schema)
      saveAsObjectFile(path, numShards, ".protobuf", metadata)
    }
  }
}
