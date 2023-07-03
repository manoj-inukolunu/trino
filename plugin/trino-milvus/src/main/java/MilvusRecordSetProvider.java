/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.inject.Inject;
import io.airlift.log.Level;
import io.milvus.response.SearchResultsWrapper;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MilvusRecordSetProvider implements ConnectorRecordSetProvider {

  private final MilvusClient milvusClient;

  @Inject
  public MilvusRecordSetProvider(MilvusClient client) {
    this.milvusClient = client;
  }

  @Override
  public RecordSet getRecordSet(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<? extends ColumnHandle> columns) {

    MilvusTableHandle tableHandle = (MilvusTableHandle) table;
    String tableName = tableHandle.getTableName();
    String databaseName = tableHandle.getSchemaName();
    milvusClient.loadCollection(databaseName, tableName);
    Map<Integer, String> colIdxToName = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      MilvusColumnHandle columnHandle = (MilvusColumnHandle) columns.get(i);
      colIdxToName.put(i, columnHandle.getColumnName());
    }
    SearchResultsWrapper resultsWrapper =
        milvusClient.getRecords(
            databaseName,
            tableName,
            columns.stream()
                .map(
                    handle -> {
                      MilvusColumnHandle columnHandle = (MilvusColumnHandle) handle;
                      return columnHandle.getColumnName();
                    })
                .collect(Collectors.toList()));

    return new MilvusRecordSet(resultsWrapper, columns, colIdxToName);
  }
}
