// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.io.index.format.sequence

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.Map
import java.lang.Iterable

import collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.Text

import scray.hdfs.io.index.format.sequence.types.Blob
import scray.hdfs.io.index.format.sequence.types.BlobKey
import scray.hdfs.io.index.format.sequence.mapping.SequneceValue
import org.apache.hadoop.io.Writable
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob

class ValueFileReader[DATAKEY <: Writable, DATAVALUE <: Writable](reader: SequenceFile.Reader, outMapping: SequneceValue[DATAKEY, DATAVALUE]) {

  private val key = new Text();
  val idxEntry = new Blob

  def this(path: String, hdfsConf: Configuration = new Configuration, fs: Option[FileSystem] = None, outMapping: SequneceValue[DATAKEY, DATAVALUE]) = {

    this(new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path)), Reader.bufferSize(4096)), outMapping)

    if (getClass.getClassLoader != null) {
      hdfsConf.setClassLoader(getClass.getClassLoader)
    }
  }

  def this(path: String, hdfsConf: Configuration, fs: Option[FileSystem], outMapping: SequneceValue[DATAKEY, DATAVALUE], hdfsConfigurationParameter: Iterable[Map.Entry[String, String]]) = {

    this(new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path)), Reader.bufferSize(4096)), outMapping)

    // Copy parameter unchecked
    hdfsConfigurationParameter
      .asScala
      .foldLeft(hdfsConf)((acc, parameter) => {
        acc.set(parameter.getKey, parameter.getValue);
        acc
      })
  }

  def this(path: String, outMapping: SequneceValue[DATAKEY, DATAVALUE]) = {
    this(path, new Configuration, None, outMapping: SequneceValue[DATAKEY, DATAVALUE])
  }

  def select(key: String): Array[Byte] = {
    Array("".toByte)
  }

  def get(keyIn: String, startPosition: Long): Option[Array[Byte]] = {
    getBlob(keyIn, 0, startPosition).map(_.getData)
  }

  def getBlobAsStream(keyIn: String, startPosition: Long): Option[InputStream] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false

    var syncSeen = false
    while (!syncSeen && !valueFound && reader.next(key, value)) {
      syncSeen = reader.syncSeen();

      if (keyIn.equals(key.getId)) {
        valueFound = true
      }
    }

    if (valueFound) {
      valueFound = false
      Some(new ByteArrayInputStream(value.getData)) // FIXME Create continous stream over all offset s...
    } else {
      None
    }
  }

  def getBlob(keyIn: String, offset: Int, startPosition: Long): Option[Blob] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false

    var syncSeen = false
    while (!syncSeen && !valueFound && reader.next(key, value)) {

      syncSeen = reader.syncSeen();

      if (keyIn.equals(key.getId) && offset == key.getOffset) {
        valueFound = true
      }
    }

    if (valueFound) {
      valueFound = false
      Some(value) // TODO test performance
    } else {
      None
    }
  }

  def getNextBlob(keyIn: String, offset: Int, startPosition: Long): Option[Tuple2[Long, Blob]] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false

    var syncSeen = false
    try {
      while (!syncSeen && !valueFound && reader.next(key, value)) {

        syncSeen = reader.syncSeen();

        if (keyIn.equals(key.getId) && offset == key.getOffset) {
          valueFound = true
        }
      }
    } catch {
      case e: java.io.EOFException => {
        print(s"No data for key ${keyIn}, offset ${offset}\n")
        e.printStackTrace()
        valueFound = false
      }
    }

    if (valueFound) {
      valueFound = false
      Some(reader.getPosition, value) // TODO test performance
    } else {
      None
    }
  }

  def getNextBlobAsStream(keyIn: String, offset: Int, startPosition: Long): Option[Tuple2[Long, InputStream]] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false

    var syncSeen = false
    while (!syncSeen && !valueFound && reader.next(key, value)) {

      syncSeen = reader.syncSeen();

      if (keyIn.equals(key.getId) && offset == key.getOffset) {
        valueFound = true
      }
    }

    if (valueFound) {
      valueFound = false
      Some(reader.getPosition, new ByteArrayInputStream(value.getData)) // TODO test performance
    } else {
      None
    }
  }

  def printBlobKeys(startPosition: Long): Unit = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob

    while (reader.next(key, value)) {
      reader.getPosition
      println(s"Key: ${key}, possition: ${reader.getPosition}")
    }
  }

  def close = {
    reader.close()
  }
}
