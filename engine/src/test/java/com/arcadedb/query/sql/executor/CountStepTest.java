package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class CountStepTest {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String COUNT_PROPERTY_NAME = "count";

  @Test
  public void shouldCountRecords() {
    CommandContext context = new BasicCommandContext();
    CountStep step = new CountStep(context, false);

    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
            InternalResultSet result = new InternalResultSet();
            if (!done) {
              for (int i = 0; i < 100; i++) {
                ResultInternal item = new ResultInternal();
                item.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    ResultSet result = step.syncPull(context, 100);
    Assertions.assertEquals(100, (long) result.next().getProperty(COUNT_PROPERTY_NAME));
    Assertions.assertFalse(result.hasNext());
  }
}
