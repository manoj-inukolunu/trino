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
import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
//import io.airlift.log.Logger;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class MilvusConnectorFactory implements ConnectorFactory {

//    private static final Logger log = Logger.get(MilvusConnectorFactory.class);
    @Override
    public String getName() {
        return "milvus";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
//        log.info("Loading Milvus Connector",config);
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
