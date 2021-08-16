package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OExplainStatementExecutionTest extends TestHelper {
  @Test
  public void testExplainSelectNoTarget() {
    ResultSet result = database.query("sql", "explain select 1 as one, 2 as two, 2+3");
    Assertions.assertTrue(result.hasNext());
    Result next = result.next();
    Assertions.assertNotNull(next.getProperty("executionPlan"));
    Assertions.assertNotNull(next.getProperty("executionPlanAsString"));

    Optional<ExecutionPlan> plan = result.getExecutionPlan();
    Assertions.assertTrue(plan.isPresent());
    Assertions.assertTrue(plan.get() instanceof SelectExecutionPlan);

    result.close();
  }
}
