import io.airlift.configuration.Config;
import java.net.URI;
import javax.validation.constraints.NotNull;

public class MilvusConfig
{
    private URI metadata;

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("metadata-uri")
    public MilvusConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }
}

