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
package scray.cassandra.extractors

import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scalaz.Memo
import scray.querying.description.ColumnConfiguration
import scray.querying.description.IndexConfiguration
import scray.querying.description.QueryspaceConfiguration
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import com.datastax.driver.core.KeyspaceMetadata
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.IndexConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.Row
import com.websudos.phantom.CassandraPrimitive
import scray.querying.description.EmptyRow
import scray.querying.description.EmptyRow
import scray.querying.queries.DomainQuery
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.ReadableStore

/**
 * Extractor object for Storehaus'-CQLCassandraCollectionStores
 */
object CQLCollectionStoreExtractor extends CassandraExtractor {

  override def getColumns(store: CQLCassandraCollectionStore[_, _, _, _, _, _]): List[Column] =
    getInternalColumns(store, store.rowkeyColumnNames ++ store.colkeyColumnNames ++ List(store.valueColumnName))
  
  override def getClusteringKeyColumns(store: CQLCassandraCollectionStore[_, _, _, _, _, _]): List[Column] =
    getInternalColumns(store, store.colkeyColumnNames)

  override def getRowKeyColumn(store: CQLCassandraCollectionStore[_, _, _, _, _, _]): Column =
    getInternalColumns(store, List(store.rowkeyColumnNames.head)).head
  
  override def getRowKeyColumns(store: CQLCassandraCollectionStore[_, _, _, _, _, _]): List[Column] =
    getInternalColumns(store, store.rowkeyColumnNames)
  
  override def getValueColumns(store: CQLCassandraCollectionStore[_, _, _, _, _, _]): List[Column] =
    getInternalColumns(store, List(store.valueColumnName))
    
  override def getTableConfiguration(store: CQLCassandraCollectionStore[_, _, _, _, _, _], rowMapper: (_) => Row): TableConfiguration[_, _] = {
    val domainQuery: DomainQuery => String = (a) => ""
    TableConfiguration[Any, Any] (
      getTableIdentifier(store), 
      // TODO: add versioning information here
      None,
      getRowKeyColumn(store),
      getClusteringKeyColumns(store),
      getColumns(store),
      rowMapper.asInstanceOf[(Any) => Row],
      domainQuery,
      () => store.asInstanceOf[QueryableStore[Any, Any]],
      () => store.asInstanceOf[ReadableStore[Any, Any]]
    )
  }
}
