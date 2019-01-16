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
import com.typesafe.scalalogging.LazyLogging

import scray.hdfs.io.read.FileParameter;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.ListenableFuture
import java.util.ArrayList
import java.util.Map
import collection.JavaConverters._

class RawFileReader(hdfsURL: String, hdfsConf: Configuration) extends LazyLogging {

  var dataReader: FileSystem = null; // scalastyle:off null
  
  if (getClass.getClassLoader != null) {
    hdfsConf.setClassLoader(getClass.getClassLoader)
  }
  
  def this(hdfsURL: String) {
    this(hdfsURL, new Configuration)
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


  
  def initReader() = {
    hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    hdfsConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");
    hdfsConf.set("fs.defaultFS", hdfsURL)

    dataReader = FileSystem.get(hdfsConf);
  }

  def read(path: String): InputStream = {
    if (dataReader == null) {
      logger.debug(s"Reader for path ${path} was not initialized. Will do it now")
      initReader
    }

    dataReader.open(new Path(path))
  }

  def deleteFile(path: String) {
    if (dataReader == null) {
      logger.debug(s"Reader for path ${path} was not initialized. Will do it now")
      initReader
    }

    dataReader.delete(new Path(path), true)
  }

  def getFileList(path: String): ListenableFuture[java.util.List[FileParameter]] = {
    val fileList = SettableFuture.create[java.util.List[FileParameter]]();

    if (dataReader == null) {
      initReader
    }

    try {
      val fileIter = dataReader.listFiles(new Path(path), false)
      val fileParameters: java.util.List[FileParameter] = new ArrayList[FileParameter](100)

      while (fileIter.hasNext()) {
        val currentFile = fileIter.next()
        val fileParamteter = new FileParameter(currentFile.getLen, path, currentFile.getPath.getName)
        fileParameters.add(fileParamteter)
      }
      fileList.set(fileParameters)
    } catch {
      case e: Throwable => {
        logger.error("Unable to get filelist")
        fileList.setException(e);
      }
    }

    return fileList;
  }
}