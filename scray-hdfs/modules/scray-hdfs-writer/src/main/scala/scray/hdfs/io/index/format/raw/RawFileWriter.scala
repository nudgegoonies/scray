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

package scray.hdfs.io.index.format.raw

import java.io.InputStream
import java.io.OutputStream
import java.util.Map

import collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.google.common.io.ByteStreams
import com.typesafe.scalalogging.LazyLogging

class RawFileWriter(hdfsURL: String, hdfsConf: Configuration) extends LazyLogging {

  var dataWriter: FileSystem = null; // scalastyle:off null

  if (getClass.getClassLoader != null) {
    hdfsConf.setClassLoader(getClass.getClassLoader)
  }

  def this(hdfsUrl: String) = {
    this(hdfsUrl, new Configuration)
  }

  def this(hdfsUrl: String, hdfsConfigurationParameter: java.lang.Iterable[Map.Entry[String, String]]) = {

    this(hdfsUrl, new Configuration)

    // Copy parameter unchecked
    hdfsConfigurationParameter
      .asScala
      .foldLeft(hdfsConf)((acc, parameter) => {
        acc.set(parameter.getKey, parameter.getValue);
        acc
      })
  }

  def initWriter(path: String): Unit = {

    hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    hdfsConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");
    hdfsConf.set("fs.defaultFS", hdfsURL)

    logger.debug(s"Create writer for path ${path}")

    dataWriter = FileSystem.get(hdfsConf);
  }

  def write(fileName: String, data: InputStream) = synchronized {
    val hdfswritepath = new Path(fileName);

    if (dataWriter == null) {
      logger.debug("Writer was not initialized. Will do it now")

      initWriter(fileName)
    }

    dataWriter.create(new Path(fileName))
    val hdfsOutputStream = dataWriter.create(hdfswritepath);

    ByteStreams.copy(data, hdfsOutputStream);
    data.close()
    hdfsOutputStream.hflush();
    hdfsOutputStream.hsync();
    hdfsOutputStream.close();
  }

  def write(fileName: String): OutputStream = {
    if (dataWriter == null) {
      logger.debug("Writer was not initialized. Will do it now")

      initWriter(fileName)
    }

    dataWriter.create(new Path(fileName))
  }

  def close = {
    dataWriter.close()
  }
}
