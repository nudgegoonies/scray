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

package scray.core.service

import com.twitter.finagle.Thrift
import scray.core.service.util.MockedPlanner
import com.twitter.util.Await
import scray.core.service.spools.TimedSpoolRack
import scala.util.Random

object ScrayTTestServer extends KryoPoolRegistration with MockedPlanner {

  val ENDPOINT = "localhost:18182"
  val ROWS = 5000
  val MAXCOLS = 50
  val bigBuf = 1.until(1000).foldLeft("")((str, int) => str + Random.nextPrintableChar)
  val VALUES = Array(1, 2, 3, 4, 1.3, 2.7, bigBuf, "foo", "bar", "baz")

  val server = Thrift.serveIface(ENDPOINT, GenTestService(ROWS, MAXCOLS, VALUES))

  def main(args : Array[String]) {
    register
    Await.ready(server)
  }
}
