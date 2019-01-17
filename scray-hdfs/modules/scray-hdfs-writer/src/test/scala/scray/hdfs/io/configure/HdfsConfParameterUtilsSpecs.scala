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

package scray.hdfs.io.configure

import java.util.Map
import java.util.AbstractMap

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.WordSpec
import scala.collection.immutable.HashMap
import java.util.ArrayList
import scala.collection.AbstractMap
import collection.JavaConversions._
import org.junit.Assert

class HdfsConfParameterUtilsSpecs  extends WordSpec with LazyLogging  {
  
  "HdfsConfParameterUtils " should {
    "set ftp host" in {
      val allowedParameters = HashMap[String, Parameter]("fs.ftp.host" -> Parameter(true, true, List("ftp.scray.org")))
      val usedParameters = new ArrayList[Map.Entry[String, String]]
      usedParameters.add(new AbstractMap.SimpleEntry("fs.ftp.host", "ftp.scray.org"))

      val hadoopConfiguration = new HdfsConfParameterUtils(allowedParameters)
      .keyValuePairsToHadoopConf(usedParameters)

      Assert.assertEquals("ftp.scray.org", hadoopConfiguration.get("fs.ftp.host"))
    }
    "skip ftp host because of wrong value" in {
      val allowedParameters = HashMap[String, Parameter]("fs.ftp.host" -> Parameter(true, true, List("ftp.scray.org")))
      val usedParameters = new ArrayList[Map.Entry[String, String]]
      usedParameters.add(new AbstractMap.SimpleEntry("fs.ftp.host", "ftp42.scray.org"))
      //usedParameters.add(new AbstractMap.SimpleEntry("dfs.client.block.write.retries", "100"))

      val hadoopConfiguration = new HdfsConfParameterUtils(allowedParameters)
      .keyValuePairsToHadoopConf(usedParameters)

      Assert.assertEquals("0.0.0.0", hadoopConfiguration.get("fs.ftp.host"))
    }
    "set defaltFS value (new key)" in {
      val allowedParameters = HashMap[String, Parameter]("fs.defaultFS" -> Parameter(true, true, List("hdfs://.scray.org")))
      val usedParameters = new ArrayList[Map.Entry[String, String]]
      usedParameters.add(new AbstractMap.SimpleEntry("fs.defaultFS", "hdfs://.scray.org"))

      val hadoopConfiguration = new HdfsConfParameterUtils(allowedParameters)
      .keyValuePairsToHadoopConf(usedParameters)

      Assert.assertEquals("hdfs://.scray.org", hadoopConfiguration.get("fs.defaultFS"))
    }
    "set not allowed defaltFS value (new key)" in {
      val allowedParameters = HashMap[String, Parameter]("fs.defaultFS" -> Parameter(true, true, List("hdfs://hdfs.scray.org")))
      val usedParameters = new ArrayList[Map.Entry[String, String]]
      usedParameters.add(new AbstractMap.SimpleEntry("fs.defaultFS", "hdfs://hdfs2.scray.org"))

      val hadoopConfiguration = new HdfsConfParameterUtils(allowedParameters)
      .keyValuePairsToHadoopConf(usedParameters)

      Assert.assertEquals("file:///", hadoopConfiguration.get("fs.defaultFS"))
    }
    "try to set not allowed key " in {
      val allowedParameters = HashMap[String, Parameter]("fs.defaultFS" -> Parameter(true, true, List("hdfs://hdfs.scray.org")))
      val usedParameters = new ArrayList[Map.Entry[String, String]]
      usedParameters.add(new AbstractMap.SimpleEntry("chicken", "hdfs://hdfs2.scray.org"))

      val hadoopConfiguration = new HdfsConfParameterUtils(allowedParameters)
      .keyValuePairsToHadoopConf(usedParameters)

      Assert.assertEquals(null, hadoopConfiguration.get("chicken"))
    }
  }
}