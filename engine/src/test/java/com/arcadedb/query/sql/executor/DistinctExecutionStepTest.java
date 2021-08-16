package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class DistinctExecutionStepTest {

  @Test
  public void test() {
    CommandContext ctx = new BasicCommandContext();
    DistinctExecutionStep step = new DistinctExecutionStep(ctx, false);

    AbstractExecutionStep prev = new AbstractExecutionStep(ctx, false) {
      boolean done = false;

      @Override
      public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        InternalResultSet result = new InternalResultSet();
        if (!done) {
          for (int i = 0; i < 10; i++) {
            ResultInternal item = new ResultInternal();
            item.setProperty("name", i % 2 == 0 ? "foo" : "bar");
            result.add(item);
          }
          done = true;
        }
        return result;
      }
    };

    step.setPrevious(prev);
    ResultSet res = step.syncPull(ctx, 10);
    Assertions.assertTrue(res.hasNext());
    res.next();
    Assertions.assertTrue(res.hasNext());
    res.next();
    Assertions.assertFalse(res.hasNext());
  }
}
