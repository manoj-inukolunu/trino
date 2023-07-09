import io.trino.spi.Page;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;

import java.util.List;
import java.util.Optional;

public class MilvusTableFunctionDataProcessor implements TableFunctionDataProcessor {
  @Override
  public TableFunctionProcessorState process(List<Optional<Page>> input) {
    return null;
  }
}
