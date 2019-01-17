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

import org.apache.hadoop.conf.Configuration
import java.util.Map
import java.lang.Iterable

import collection.JavaConverters._
import scala.collection.immutable.HashMap
import com.typesafe.scalalogging.LazyLogging

case class Parameter(isAllowd: Boolean, valuesAreLimited: Boolean, allowedValues: List[String])

class HdfsConfParameterUtils(allowedParameter: HashMap[String, Parameter]) extends LazyLogging {

  def keyValuePairsToHadoopConf(hdfsClientParameters: Iterable[Map.Entry[String, String]]): Configuration = {
    hdfsClientParameters
      .asScala
      .foldLeft(new Configuration)((hadoopConfiguration, parameter) => {
        if (isAllowed(parameter)) {
          hadoopConfiguration.set(parameter.getKey, parameter.getValue);
          hadoopConfiguration
        } else {
          logger.warn(s"Not allow Hadoop configuration parameter used ${parameter}")
          hadoopConfiguration
        }
      })
  }

  protected def isAllowed(param: Map.Entry[String, String]): Boolean = {

    allowedParameter.get(param.getKey.trim())
      .map(knownParm => {
        if (knownParm.isAllowd) {
          if (knownParm.valuesAreLimited) {
            if (knownParm.allowedValues.filter(_.equals(param.getValue)).isEmpty) {
              logger.debug(s"Given value (${param}) for key ${param.getKey} not allowed")
              return false
            } else {
              return true
            }
          } else {
            return true
          }
        } else {
          logger.debug(s"Forbidden parameter ${param}")
          return false
        }
      })

    logger.debug(s"Unknown property name (${param.getKey}) for Hadoop configuration")
    return false
  }
}