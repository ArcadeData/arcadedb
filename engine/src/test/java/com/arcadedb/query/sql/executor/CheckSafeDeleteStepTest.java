package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckSafeDeleteStepTest {

  @Test
  public void shouldSafelyDeleteRecord() throws Exception {
    TestHelper.executeInNewDatabase((database) -> {
      CommandContext context = new BasicCommandContext();
      CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
      AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
        boolean done = false;

        @Override
        public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
          InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              ResultInternal item = new ResultInternal();
              item.setElement(database.newDocument(TestHelper.createRandomType(database).getName()));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      ResultSet result = step.syncPull(context, 10);
      Assertions.assertEquals(10, result.stream().count());
      Assertions.assertFalse(result.hasNext());
    });
  }
}
