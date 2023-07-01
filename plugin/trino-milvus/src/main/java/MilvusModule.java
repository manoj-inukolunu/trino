
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class MilvusModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MilvusConnector.class).in(Scopes.SINGLETON);
        binder.bind(MilvusMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MilvusClient.class).in(Scopes.SINGLETON);
        binder.bind(MilvusSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MilvusRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MilvusConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(MilvusTable.class));
    }
}

