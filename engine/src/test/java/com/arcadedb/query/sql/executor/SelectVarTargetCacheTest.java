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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #6669: {@code SelectExecutionPlanner.init()} aliased the statement's FROM target instead
 * of copying it, so a later rewrite of a {@code $var} target (from
 * {@code SelectExecutionPlanner#rewriteIndexChainsAsSubqueries}) mutated the FromClause node in place. That
 * node is the exact object {@link com.arcadedb.query.sql.parser.StatementCache} hands back for every execution
 * of the same SQL text, so the first execution's resolved type stuck permanently to the cached statement - and,
 * via {@link com.arcadedb.query.sql.parser.ExecutionPlanCache} (also keyed purely by SQL text, independent of
 * any variable binding), to the cached execution plan as well.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SelectVarTargetCacheTest extends TestHelper {

  @Test
  void reusedStatementWithDifferentVarTargetReturnsFreshType() {
    database.getSchema().createDocumentType("TypeA");
    database.getSchema().createDocumentType("TypeB");

    database.begin();
    database.newDocument("TypeA").set("name", "fromA").save();
    database.newDocument("TypeB").set("name", "fromB").save();
    database.commit();

    final String sqlText = "SELECT FROM $t";
    final DatabaseInternal db = (DatabaseInternal) database;

    // First execution: $t = TypeA
    final SelectStatement stmt1 = (SelectStatement) db.getStatementCache().get(sqlText);
    final BasicCommandContext ctx1 = new BasicCommandContext();
    ctx1.setDatabase(database);
    ctx1.setVariable("$t", "TypeA");
    final ResultSet rs1 = new LocalResultSet(stmt1.createExecutionPlan(ctx1));
    assertThat(rs1.hasNext()).isTrue();
    assertThat(rs1.next().<String>getProperty("name")).isEqualTo("fromA");
    rs1.close();

    // Second execution: StatementCache.get() returns the exact same parsed Statement instance for identical
    // SQL text - proving this reuses the shared, potentially-mutated AST rather than a fresh parse.
    final SelectStatement stmt2 = (SelectStatement) db.getStatementCache().get(sqlText);
    assertThat(stmt2).isSameAs(stmt1);

    final BasicCommandContext ctx2 = new BasicCommandContext();
    ctx2.setDatabase(database);
    ctx2.setVariable("$t", "TypeB");
    final ResultSet rs2 = new LocalResultSet(stmt2.createExecutionPlan(ctx2));
    assertThat(rs2.hasNext()).isTrue();
    assertThat(rs2.next().<String>getProperty("name")).isEqualTo("fromB");
    rs2.close();

    // Third execution: back to TypeA. Reuses the same shared parsed Statement once more (not the
    // ExecutionPlanCache: a $var target is non-cacheable, so createExecutionPlan() never consults that cache
    // for this statement at all - see the assertion below), which must not still be pinned to whichever type
    // built the first execution's plan.
    assertThat(stmt1.executionPlanCanBeCached()).isFalse();
    final SelectStatement stmt3 = (SelectStatement) db.getStatementCache().get(sqlText);
    final BasicCommandContext ctx3 = new BasicCommandContext();
    ctx3.setDatabase(database);
    ctx3.setVariable("$t", "TypeA");
    final ResultSet rs3 = new LocalResultSet(stmt3.createExecutionPlan(ctx3));
    assertThat(rs3.hasNext()).isTrue();
    assertThat(rs3.next().<String>getProperty("name")).isEqualTo("fromA");
    rs3.close();
  }

  @Test
  void ordinaryTargetStillUsesExecutionPlanCache() {
    // Companion to the test above: confirms marking a $var target non-cacheable in FromItem.isCacheable()
    // didn't overreach and disable plan caching for a plain, literal-type target.
    database.getSchema().createDocumentType("TypeA");
    database.begin();
    database.newDocument("TypeA").set("name", "fromA").save();
    database.commit();

    final String sqlText = "SELECT FROM TypeA";
    final DatabaseInternal db = (DatabaseInternal) database;
    final SelectStatement stmt = (SelectStatement) db.getStatementCache().get(sqlText);
    assertThat(stmt.executionPlanCanBeCached()).isTrue();

    // ExecutionPlanCache.put() discards a plan built before the cache's own invalidation timestamp - both are
    // millisecond System.currentTimeMillis() reads, so a plan built in the very same millisecond as the schema
    // DDL above (which invalidates the cache) can lose that race under a busy JVM. Wait past it explicitly
    // rather than asserting on elapsed time (see StallAwareStopwatch guidance): this loop bounds nothing, it
    // just guarantees the plan is built strictly after the invalidation it must not lose to.
    final long lastInvalidation = db.getExecutionPlanCache().getLastInvalidation();
    while (System.currentTimeMillis() <= lastInvalidation) {
      Thread.onSpinWait();
    }

    assertThat(db.getExecutionPlanCache().contains(sqlText)).isFalse();
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(database);
    stmt.createExecutionPlan(ctx);
    assertThat(db.getExecutionPlanCache().contains(sqlText)).isTrue();
  }
}
