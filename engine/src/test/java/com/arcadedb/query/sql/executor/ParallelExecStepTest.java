package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class ParallelExecStepTest {

  @Test
  public void test() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      CommandContext ctx = new BasicCommandContext();
      List<InternalExecutionPlan> subPlans = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        FetchFromRidsStep step0 = new FetchFromRidsStep(Collections.singleton(new RID(db, 12, i)), ctx, false);
        FetchFromRidsStep step1 = new FetchFromRidsStep(Collections.singleton(new RID(db, 12, i)), ctx, false);
        InternalExecutionPlan plan = new SelectExecutionPlan(ctx);
        plan.getSteps().add(step0);
        plan.getSteps().add(step1);
        subPlans.add(plan);
      }

      ParallelExecStep step = new ParallelExecStep(subPlans, ctx, false);

      SelectExecutionPlan plan = new SelectExecutionPlan(ctx);
      plan.getSteps().add(new FetchFromRidsStep(Collections.singleton(new RID(db, 12, 100)), ctx, false));
      plan.getSteps().add(step);
      plan.getSteps().add(new FetchFromRidsStep(Collections.singleton(new RID(db, 12, 100)), ctx, false));
    });
  }
}
