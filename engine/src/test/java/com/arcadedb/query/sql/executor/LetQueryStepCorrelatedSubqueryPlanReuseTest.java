/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LetQueryStep} re-planned its correlated subquery from scratch on every incoming row (calling
 * {@code Statement#createExecutionPlan} again each time), even though the query text never changes between rows -
 * only the {@code $parent} binding a correlated subquery reads does. The fix builds the sub-plan once and reuses it
 * for later rows via {@link InternalExecutionPlan#copy}, the same mechanism the top-level statement cache already
 * relies on, which gives every row its own fresh step instances bound to that row's context.
 * <p>
 * The risk that reuse introduces is a stale {@code $parent} binding leaking from one row into another row's result.
 * This test's job is to catch exactly that: several independent chains, each leaf asserting it resolves its OWN
 * parent/root and not a neighbour's, under both plain and PROFILE execution.
 */
class LetQueryStepCorrelatedSubqueryPlanReuseTest extends TestHelper {

  private static final int    CHAINS = 4;
  private static final String QUERY  =
      "select name, $uf.name as parentName, $root[0].name as rootName " +
          "from PlanReuseNode " +
          "let " +
          "  $uf = out('PlanReuseNode_parent')[0], " +
          "  $root = (select from (traverse out('PlanReuseNode_parent') from (select $parent.uf)) where isRoot = true) " +
          "where name like 'Leaf%' " +
          "order by name";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("PlanReuseNode");
    database.getSchema().createEdgeType("PlanReuseNode_parent");

    database.transaction(() -> {
      for (int i = 1; i <= CHAINS; i++) {
        database.command("sql", "create vertex PlanReuseNode set name = 'Root" + i + "', isRoot = true").close();
        database.command("sql", "create vertex PlanReuseNode set name = 'Mid" + i + "'").close();
        database.command("sql", "create vertex PlanReuseNode set name = 'Leaf" + i + "'").close();
        database.command("sql",
            "create edge PlanReuseNode_parent from (select from PlanReuseNode where name = 'Mid" + i
                + "') to (select from PlanReuseNode where name = 'Root" + i + "')").close();
        database.command("sql",
            "create edge PlanReuseNode_parent from (select from PlanReuseNode where name = 'Leaf" + i
                + "') to (select from PlanReuseNode where name = 'Mid" + i + "')").close();
      }
    });
  }

  @Test
  void everyRowResolvesItsOwnChainNotAnotherRows() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", QUERY);
      for (int i = 1; i <= CHAINS; i++) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("Leaf" + i);
        assertThat(row.<String>getProperty("parentName")).isEqualTo("Mid" + i);
        assertThat(row.<String>getProperty("rootName")).isEqualTo("Root" + i);
      }
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  // PROFILE drains the result set internally (ProfileStatement#execute) and returns only the execution plan, never
  // the data rows - so this only has to prove profiled execution completes and produces a real, timed plan; row
  // correctness is covered by everyRowResolvesItsOwnChainNotAnotherRows.
  @Test
  void executesCleanlyUnderProfile() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "PROFILE " + QUERY);
      assertThat(rs.getExecutionPlan()).isPresent();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("μs");
      rs.close();
    });
  }

  @Test
  void secondAndLaterRowsReuseTheFirstRowsPlanTemplate() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", QUERY);
      while (rs.hasNext())
        rs.next();

      final ExecutionPlan plan = rs.getExecutionPlan().get();
      final LetQueryStep letStep = findRootLetStep(plan);
      assertThat(letStep).as("LET $root step").isNotNull();

      final Object cached;
      try {
        final Field field = LetQueryStep.class.getDeclaredField("cachedSubPlanTemplate");
        field.setAccessible(true);
        cached = field.get(letStep);
      } catch (final ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
      assertThat(cached).as("plan template cached after the first row").isNotNull();

      rs.close();
    });
  }

  private LetQueryStep findRootLetStep(final ExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof LetQueryStep letQueryStep)
        return letQueryStep;
    return null;
  }
}
