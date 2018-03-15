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

package scray.hdfs.index.format.sequence.types

import java.io.InputStream
import java.io.IOException
import org.apache.hadoop.io.SequenceFile
import scray.hdfs.index.format.sequence.BlobFileReader
import java.util.Arrays
import com.typesafe.scalalogging.LazyLogging

case class SplittetSequenceFilePossition(splittOffset: Int, possitionInFile: Long)

class BlobInputStream(reader: BlobFileReader, index: IndexValue) extends InputStream with LazyLogging {
  var readPossitionInBuffer = -1
  var dataBuffer: Array[Byte] = null
  var possitionInFile = SplittetSequenceFilePossition(0, index.getPosition)
  var eOFReached = false;

  override def read: Int = {

    if (dataBuffer == null) {
      updateState(updateBuffer(possitionInFile))
    }

    if ((readPossitionInBuffer + 1) < dataBuffer.size) {
      readPossitionInBuffer = readPossitionInBuffer + 1

      dataBuffer(readPossitionInBuffer)
    } else {
      this.updateState(updateBuffer(possitionInFile))

      if (eOFReached) {
        -1
      } else {
        readPossitionInBuffer = readPossitionInBuffer + 1
        dataBuffer(readPossitionInBuffer)
      }

    }
  }

  override def read(b: Array[Byte]): Int = {
    this.read(b, 0, b.length)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    var writtenBytes = 0

    if (dataBuffer == null) {
      updateState(updateBuffer(possitionInFile))
    }

    if (dataBuffer.length == readPossitionInBuffer) {
      updateState(updateBuffer(possitionInFile))
    }

    // Multiple blobs required to fill requested buffer
    var numElementsInBuffer = (dataBuffer.length) - readPossitionInBuffer
    var outputBytes = 0 // Number of bytes written to output buffer
    var posInOutputBuffer = 0
    
    if (numElementsInBuffer < len) {
        logger.debug(s"Multiple splits required to fill requested buffer. Bytes in current buffer ${numElementsInBuffer}. Requested butes ${len}")

      while (outputBytes < len && !eOFReached) {

        numElementsInBuffer = (dataBuffer.length) - readPossitionInBuffer
        readPossitionInBuffer = readPossitionInBuffer + 1
        writtenBytes = writtenBytes + numElementsInBuffer

        System.arraycopy(
          dataBuffer,
          readPossitionInBuffer,
          b,
          posInOutputBuffer,
          (numElementsInBuffer -1))
          
          posInOutputBuffer = posInOutputBuffer + numElementsInBuffer -1

        outputBytes = outputBytes + numElementsInBuffer -1
        logger.debug(s"Wrote ${outputBytes} bytes")
          this.updateState(updateBuffer(possitionInFile))
      }
    }

    if (dataBuffer.length == readPossitionInBuffer) {
      updateState(updateBuffer(possitionInFile))
    }

    if(outputBytes == 0) {
      -1
    } else {
      outputBytes
    }
  }

  def readTillInputStreamBufferIsFull(dataBuffer: Array[Byte], readPossitionInBuffer: Int, outputData: Array[Byte], readBytes: Int, len: Int) {

    if (readBytes >= len) {
      outputData
    } else {
      val numElementsInBuffer = (dataBuffer.size - readPossitionInBuffer)

      System.arraycopy(dataBuffer, readPossitionInBuffer, outputData, readBytes, numElementsInBuffer)

      updateState(updateBuffer(possitionInFile))
      this.readTillInputStreamBufferIsFull(dataBuffer, readPossitionInBuffer, outputData, readBytes, len)
    }
  }
  override def skip(n: Long): Long = {
    // FIXME 
    n
  }

  override def available: Int = {
    0
  }

  override def close = {
    reader.close
  }
  override def mark(readlimit: Int) = {

  }

  override def reset = {
    throw new IOException()
  }

  override def markSupported: Boolean = {
    false
  }

  private def updateBuffer(pos: SplittetSequenceFilePossition): Option[Tuple2[SplittetSequenceFilePossition, Array[Byte]]] = {
    if (pos.splittOffset <= index.getBlobSplits) {
      reader.getNextBlob(index.getKey, pos.splittOffset, pos.possitionInFile)
        .map {
          case (newPossiton, data) =>
            (SplittetSequenceFilePossition(pos.splittOffset + 1, newPossiton), data.getData)
        }
    } else {
      None
    }
  }

  private def updateState(data: Option[Tuple2[SplittetSequenceFilePossition, Array[Byte]]]) = {
    data.map {
      case (pos, data) =>
        possitionInFile = pos
        dataBuffer = data
        readPossitionInBuffer = -1
    }

    if (!data.isDefined) {
      possitionInFile = SplittetSequenceFilePossition(0, 0L)
      dataBuffer = Array[Byte]()
      readPossitionInBuffer = -1
      this.eOFReached = true
    }
  }
}