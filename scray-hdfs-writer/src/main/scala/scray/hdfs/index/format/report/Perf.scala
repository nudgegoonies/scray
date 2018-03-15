package scray.hdfs.index.format.report

import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.orc.ORCFileWriter

object Perf {


	def test1(url : String, size : Int) {

		//val writer = new SequenceFileWriter(url)

		val writer = new ORCFileWriter(url)


				val insertStart = System.currentTimeMillis();

		for (i <- 0 to size) {
			val key = "key_" + i
					val value = "data_" + i

					//println(s"Write key value data. key=${key}, value=${value}")

					writer.insert(key, System.currentTimeMillis(), value.getBytes)
		}

		//writer.flush()
		val insertEnd = System.currentTimeMillis();
		System.out.println(size, insertEnd - insertStart);

		writer.close
	}

	def main(args: Array[String]) {

	  
	  /* import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF) */
	  
	  import ch.qos.logback.classic.{Logger, Level}
import org.slf4j.LoggerFactory

val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
rootLogger.setLevel(Level.OFF)
	  
	  
		if (args.size == 0) {
			println("No HDFS URL defined. E.g. hdfs://127.0.0.1/user/scray/scray-hdfs-data/")
		} else {
			//val url: String = s"${args(0)}/scray-data-${System.currentTimeMillis()}"
			val s = args(1)
					//test1(url, 1)

					//val s = "2, 1000000, 1, 1,   1"
					val k : Array[Int] = s.split(",").map(_.trim.toInt)
					
					k.foreach(p => { val url: String = s"${args(0)}/scray-data-${System.currentTimeMillis()}";
					test1(url,p)
					  })
		}
	}
}