import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;

public class MilvusTableFunction extends AbstractConnectorTableFunction {
  public MilvusTableFunction(
      String schema,
      String name,
      List<ArgumentSpecification> arguments,
      ReturnTypeSpecification returnTypeSpecification) {
    super(schema, name, arguments, returnTypeSpecification);
  }

  @Override
  public TableFunctionAnalysis analyze(
      ConnectorSession session,
      ConnectorTransactionHandle transaction,
      Map<String, Argument> arguments,
      ConnectorAccessControl accessControl) {
    TableArgument table = (TableArgument) arguments.get("table");
    return null;
  }
}
