import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class MilvusConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "miluvs-connector";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");
        checkSpiVersion(context, this);

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new MilvusModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(MilvusConnector.class);
    }
}
