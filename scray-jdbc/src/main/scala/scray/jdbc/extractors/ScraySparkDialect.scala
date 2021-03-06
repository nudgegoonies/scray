package scray.jdbc.extractors

import scray.querying.description.internal.Domain
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange

/**
 * Spark Thrift Server dialect for Scray
 */
object ScraySparkDialect extends ScraySQLDialect("SPARK") {
  
  /**
   * Oracle implements limits by introducing an implicit column
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]]) = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    if(range.skip.isDefined || range.limit.isDefined) {
      sbuf.append(" LIMIT ")
      if(range.skip.isDefined && !range.limit.isDefined) {
        // according to mysql docu append large number to retrieve 
        // all rows if skip will be defined only
        sbuf.append("18446744073709551615")
      } else {
        sbuf.append(s"${range.limit.get}")
      }
      range.skip.foreach { skip =>
        // offsets / skips start from zero in mysql
        sbuf.append(s" OFFSET ${skip}")
      }
    }
    (sbuf.toString, List())
  }.getOrElse(("", List()))

  /**
   * Because Oracle has a special way of handling limits we need a special SELECT clause for it
   */
  override def getFormattedSelectString(table: TableIdentifier, where: String, limit: String,
      groupBy: String, orderBy: String): String =
    s"""SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy} ${limit} """

  
  /**
   * Oracle has the special overly bad habit of treating empty Strings as to be NULLs
   * TODO: Probably we need to account for that in the query generation! 
   */
  override def emptyStringIsNull: Boolean = false
  
  /**
   * we scan if the URL is of format:
   * jdbc:hive2:// ...
   * 
   * correct format according to 
   * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-thrift-server.html is:
   * jdbc:hive2://localhost:10000
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:HIVE2://")
  
  override val DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver"
}
