package scray.hdfs.io.read

import java.io.InputStream
import java.util.Map
import java.lang.Iterable

import scray.hdfs.io.write.ScrayListenableFuture

trait ReadService {
  def getInputStream(path: String): ScrayListenableFuture[InputStream]
  /**
   * 
   * @param hdfsClientParameters Set hdfs specific parameters <a href="https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml">hdfs-default.xml</a>. Allowed parameters can be configured in  configuration-parameters.property
   */
  def getInputStream(path: String, hdfsClientParameters: Iterable[Map.Entry[String, String]]): ScrayListenableFuture[InputStream]
  def getFileList(path: String): ScrayListenableFuture[java.util.List[FileParameter]]
  
  
  /**
   * 
   * @param hdfsClientParameters Set hdfs specific parameters <a href="https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml">hdfs-default.xml</a>. Allowed parameters can be configured in  configuration-parameters.property
   */
  def getFileList(path: String, hdfsClientParameters: Iterable[Map.Entry[String, String]]): ScrayListenableFuture[java.util.List[FileParameter]]

  def deleteFile(path: String): ScrayListenableFuture[String]
  
  /**
   * 
   * @param hdfsClientParameters Set hdfs specific parameters <a href="https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml">hdfs-default.xml</a>. Allowed parameters can be configured in  configuration-parameters.property
   */
  def deleteFile(path: String, hdfsClientParameters: Iterable[Map.Entry[String, String]]): ScrayListenableFuture[String]
}