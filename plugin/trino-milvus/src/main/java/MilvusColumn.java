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
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class MilvusColumn {
  private final String name;
  private final Type type;

  private String extraInfo;

  @JsonCreator
  public MilvusColumn(
      @JsonProperty("name") String name,
      @JsonProperty("type") Type type,
      @JsonProperty("extraInfo") String extraInfo) {
    checkArgument(!isNullOrEmpty(name), "name is null or is empty");
    this.name = name;
    this.type = requireNonNull(type, "type is null");
    this.extraInfo = extraInfo;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getExtraInfo() {
    return extraInfo;
  }

  @JsonProperty
  public Type getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    MilvusColumn other = (MilvusColumn) obj;
    return Objects.equals(this.name, other.name) && Objects.equals(this.type, other.type);
  }

  @Override
  public String toString() {
    return name + ":" + type;
  }
}
