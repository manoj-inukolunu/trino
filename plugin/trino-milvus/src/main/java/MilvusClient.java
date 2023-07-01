
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MilvusClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, MilvusTable>>> schemas;

    @Inject
    public MilvusClient(MilvusConfig config, JsonCodec<Map<String, List<MilvusTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");
        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, MilvusTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public MilvusTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, MilvusTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, MilvusTable>>> schemasSupplier(JsonCodec<Map<String, List<MilvusTable>>> catalogCodec, URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, MilvusTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<MilvusTable>>> catalogCodec)
            throws IOException
    {
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        Map<String, List<MilvusTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<MilvusTable>, Map<String, MilvusTable>> resolveAndIndexTables(URI metadataUri)
    {
        return tables -> {
            Iterable<MilvusTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, MilvusTable::getName));
        };
    }

    private static Function<MilvusTable, MilvusTable> tableUriResolver(URI baseUri)
    {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new MilvusTable(table.getName(), table.getColumns(), sources);
        };
    }
}

