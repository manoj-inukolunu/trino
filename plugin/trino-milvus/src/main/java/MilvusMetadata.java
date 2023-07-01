import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import java.util.Optional;

public class MilvusMetadata implements ConnectorMetadata {


    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                                                                                   ConnectorTableHandle handle,
                                                                                   Constraint constraint) {

        return ConnectorMetadata.super.applyFilter(session, handle, constraint);
    }
}
