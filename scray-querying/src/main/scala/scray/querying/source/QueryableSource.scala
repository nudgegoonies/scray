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
package scray.querying.source

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore}
import com.twitter.util.{Future, Throw, Return}
import scray.querying.description.{Column, Row}
import scray.querying.queries.DomainQuery
import scray.querying.Registry
import scray.querying.description.TableIdentifier

/**
 * queries a Storehaus-store. Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class QueryableSource[K, V](store: QueryableStore[K, V], space: String, table: TableIdentifier) 
    extends LazySource[DomainQuery] {

  val valueToRow: (V) => Row = Registry.querySpaceTables.get(space).get.
    get(table).get.rowMapper.asInstanceOf[(V) => Row]
  
  val queryMapping: DomainQuery => K = Registry.querySpaceTables.get(space).get.
    get(table).get.domainQueryMapping.asInstanceOf[DomainQuery => K]
  
  override def request(query: DomainQuery): Future[Spool[Row]] = store.queryable.get(queryMapping(query)).transform {
    case Throw(y) => Future.exception(y)
    case Return(x) => 
      // construct lazy spool
      QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow)
  }

  override def getColumns: List[Column] = {
    Registry.querySpaceTables.get(space).get.
      get(table).get.allColumns
  }
  
  /**
   * looks up in the registry if we can fulfill the ordering
   */
  override def isOrdered(query: DomainQuery): Boolean = {
    query.getOrdering match {
      case Some(col) => Registry.querySpaceColumns.get(space).get.get(col.column) match {
          case None => false
          case Some(colConfig) => colConfig.index.map(_.isSorted).orElse(Some(false)).get
        }
      case None => false
    }
  }
}

object QueryableSource {
  
  /**
   * copied from com.twitter.storehaus.IterableStore, but removed tuple dependency
   */
  private def iteratorToSpool[V](it: Iterator[V], transformer: (V) => Row): Future[Spool[Row]] = Future.value {
    if (it.hasNext) {
      // *:: for lazy/deferred tail
      transformer(it.next) *:: iteratorToSpool(it, transformer)
    } else {
      Spool.empty
    }
  }
}
