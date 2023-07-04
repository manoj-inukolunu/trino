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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MilvusColumnHandle implements ColumnHandle {
  private final String columnName;

  private final Type columnType;

  private final int ordinalPosition;
  private final String extraInfo;

  @JsonCreator
  public MilvusColumnHandle(
      @JsonProperty("columnname") String columnName,
      @JsonProperty("columnType") Type columnType,
      @JsonProperty("extraInfo") String extraInfo,
      @JsonProperty("ordinamPosition") int ordinalPosition) {

    this.columnName = requireNonNull(columnName, "columnName is null");
    this.columnType = requireNonNull(columnType, "columnType is null");
    this.ordinalPosition = ordinalPosition;
    this.extraInfo = extraInfo;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public Type getColumnType() {
    return columnType;
  }

  @JsonProperty
  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  @JsonProperty
  public String getExtraInfo() {
    return extraInfo;
  }

  public ColumnMetadata getColumnMetadata() {
    return ColumnMetadata.builder()
        .setName(columnName)
        .setType(columnType)
        .setExtraInfo(Optional.ofNullable(extraInfo))
        .build();
  }

  @Override
  public int hashCode() {
    return columnName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    MilvusColumnHandle other = (MilvusColumnHandle) obj;
    return columnName.equals(other.columnName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("columnName", columnName)
        .add("columnType", columnType)
        .add("ordinalPosition", ordinalPosition)
        .add("extraInfo", extraInfo)
        .toString();
  }
}
