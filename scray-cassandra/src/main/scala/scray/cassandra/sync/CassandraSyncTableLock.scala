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

package scray.cassandra.sync

import java.util.concurrent.TimeUnit

import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder}
import org.slf4j.LoggerFactory
import scray.querying.sync._
import scray.querying.sync.conf.SyncConfiguration

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder

import scray.querying.sync.JobInfo
import scray.querying.sync.DbSession
import scray.querying.sync.LockApi
import scray.querying.sync.SyncTableBasicClasses
import scray.querying.sync.Table
import scray.querying.sync.UnableToLockJobError
import com.typesafe.scalalogging.LazyLogging


class CassandraSyncTableLock (job: JobInfo[Statement, Insert, ResultSet], jobLockTable: Table[SyncTableBasicClasses.JobLockTable], 
  dbSession: DbSession[Statement, Insert, ResultSet], val timeOut: Int) extends LockApi[Statement, Insert, ResultSet](job, jobLockTable, dbSession) with LazyLogging {

  
  val timeBetweenRetries = 100 // ms
  
  @transient
  lazy val lockQuery = getLockQuery
  
  @transient    
  lazy val unLockQuery = getUnlockQuery
  
   
  class CassandraSessionBasedDBSession(cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet](cassandraSession.getCluster.getMetadata.getAllHosts().iterator().next.getAddress.toString) {

    override def execute(statement: String): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: SimpleStatement): Try[ResultSet] = {
       try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }
    
    def printStatement(s: Statement): String = {
      s match {
         case bStatement: BatchStatement => "It is currently not possible to execute : " + bStatement.getStatements
         case _                          => "It is currently not possible to execute : " + s
      }
    }
  }
  
    
    def getLockQuery: Statement = {
     val query = QueryBuilder.update(jobLockTable.keySpace, jobLockTable.tableName)
    .`with`(QueryBuilder.set(jobLockTable.columns.locked.name, true))
    .where(QueryBuilder.eq(jobLockTable.columns.jobname.name, job.name))  

      // Set consistency level for lightweight transactions
      job.syncConf.versionUpdateConsitencyLevel match {
         case scray.querying.sync.conf.ConsistencyLevel.LOCAL_SERIAL => {
           query.onlyIf(QueryBuilder.eq(jobLockTable.columns.locked.name, false))
           query.setSerialConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL)
         }
         case _ => // No action required
       }

    query
  }
    
       
  def getUnlockQuery = {
    @transient
    lazy val query = QueryBuilder.update(jobLockTable.keySpace, jobLockTable.tableName)
    .`with`(QueryBuilder.set(jobLockTable.columns.locked.name, false))
    .where(QueryBuilder.eq(jobLockTable.columns.jobname.name, job.name))

    // Set consistency level for lightweight transactions
    job.syncConf.versionUpdateConsitencyLevel match {
      case scray.querying.sync.conf.ConsistencyLevel.LOCAL_SERIAL => {
        query.onlyIf(QueryBuilder.eq(jobLockTable.columns.locked.name, true))
        query.setSerialConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL)
      }
      case _ => // No action required
    }

    query
  }
  
  def lock(): Unit = {
    @tailrec 
    def tryToLock: Unit = {
      try {
        while(!tryLock()){
          logger.debug(s"Retry to get lock for job ${job.name} in ${timeBetweenRetries}ms")
          Thread.sleep(timeBetweenRetries)
        }
      } catch {
        case e: InterruptedException => tryToLock
        case e: Throwable => throw e
      }
    }
    tryToLock
  }
  
  def lockInterruptibly(): Unit = {
    if(Thread.interrupted()) {
      throw new InterruptedException(s"Lock aquisition for job ${job.name} was interupted.")
    }
    
    while(!tryLock()){}
  }
  
  def tryLock(time: Long, unit: java.util.concurrent.TimeUnit): Boolean = {
    if(Thread.interrupted()) {
      throw new InterruptedException(s"Lock aquisition for job ${job.name} was interupted.")
    }

    val endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(time, unit)

    @tailrec 
    def tryToLockR(): Boolean = {
      if(tryLock()) {
        true
      } else {
        if(System.currentTimeMillis() >= endTime) {
          logger.debug(s"Retry to get lock for job ${job.name} in ${timeBetweenRetries}ms. Give up in ${TimeUnit.SECONDS.convert((endTime - System.currentTimeMillis()), TimeUnit.MICROSECONDS)}")
          Thread.sleep(timeBetweenRetries)
          tryToLockR()
        } else {
          false
        }
      }
    }
    
    tryToLockR
  }
  def tryLock(): Boolean = {
    executeQuorum(lockQuery).isSuccess
  }
  
  def unlock(): Unit = {
    executeQuorum(unLockQuery)
  }
  
  def newCondition(): java.util.concurrent.locks.Condition = ???
  
  def lockJob: Try[Unit] = {
   Try(this.lock())
  }
  
  def unlockJob(): Try[Unit] = {
    Try(unlock())
  }
  
  def transaction[ResultT](f: () => Try[ResultT]): Try[ResultT] = {
    logger.debug(s"Start transaction for job ${this.job.name}")
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      matchResult(f())
    } else {
     logger.warn(s"Unable to lock job ${job.name}")
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, ResultT](f: (P1) => Try[ResultT], p1: P1): Try[ResultT] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      matchResult(f(p1)) 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2, ResultT](f: (P1, P2) => Try[ResultT], p1: P1, p2: P2): Try[ResultT] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      matchResult(f(p1, p2))
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2, P3, ResultT](f: (P1, P2, P3) => Try[ResultT], p1: P1, p2: P2, p3: P3): Try[ResultT] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      matchResult(f(p1, p2, p3))
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2, P3, P4, ResultT](f: (P1, P2, P3, P4) => Try[ResultT], p1: P1, p2: P2, p3: P3, p4: P4): Try[ResultT] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      matchResult(f(p1, p2, p3, p4)) 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def matchResult[ResultT](result: Try[ResultT]) = {
    result match {
      case Success(result) => this.unlock(); Success(result) 
      case Failure(ex) => {
        logger.error(s"Unable to execute Query. Release lock for job ${job.name}")
        this.unlock()
        Failure(ex)}
    } 
  }
  private def executeQuorum(statement: Statement): Try[Unit] = {

    statement match {
      case bStatement: BatchStatement => logger.debug("Execute batch statement: " + bStatement.getStatements)
      case _                          => logger.debug("Execute query: " + statement)
    }
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM)

    dbSession.execute(statement).recoverWith {
      case ex: Throwable => {
          logger.error(s"Error while executing statement: ${statement}. ${ex.printStackTrace()} \t ${ex.getMessage}}")
          Failure(new StatementExecutionError(ex.getLocalizedMessage))
      } 
    }.map(_ => Unit)
  }
}

object CassandraSyncTableLock extends Serializable {
  def apply(
      job: JobInfo[Statement, Insert, ResultSet], 
      jobLockTable: Table[SyncTableBasicClasses.JobLockTable], 
      dbSession: DbSession[Statement, Insert, ResultSet],
      timeOut: Int) = {
    new CassandraSyncTableLock(job, jobLockTable, dbSession, timeOut)
  }
}