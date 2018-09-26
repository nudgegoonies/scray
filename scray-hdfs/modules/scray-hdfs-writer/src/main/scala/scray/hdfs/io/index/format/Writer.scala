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

package scray.hdfs.io.index.format

import scray.hdfs.io.index.format.sequence.types.Blob
import java.io.InputStream
import java.math.BigInteger
import java.util.UUID

trait Writer { 
  var varIsClosed = false
  
  def insert(id: String, updateTime: Long, data: Array[Byte]): Long
  def insert(id: String, data: String): Long
  def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): Long
  def insert(id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): Long
  def getPath: String
  def getBytesWritten: Long
  def getNumberOfInserts: Int
  def close
  
  def isClosed = {
    varIsClosed
  }
}
