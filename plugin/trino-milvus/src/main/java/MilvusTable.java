
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import java.net.URI;
import java.util.List;

public class MilvusTable
{
    private final String name;
    private final List<MilvusColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<URI> sources;

    @JsonCreator
    public MilvusTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<MilvusColumn> columns,
            @JsonProperty("sources") List<URI> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (MilvusColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<MilvusColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<URI> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}

