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
import com.arcadedb.index.IndexException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9331, the restart path: a point lookup that overlapped a commit starts again
 * ({@code GetValueFromIndexEntryStep.restartLookup}), which resets the index step, and a reset step resolves its index again
 * by name. When a concurrent {@code DROP INDEX} took the index away in between, both reads of the name raised the
 * {@code SchemaException} "Index with name ... was not found", which the re-planning of the statement
 * ({@code Statement.openResultSetPlanningAgainOnStaleIndex}) does not catch, so a read-only SELECT failed instead of being
 * planned again (seen once in ~600k statements of {@code Issue9331Issue9332IndexDdlDuringReadsTest}).
 */
class Issue9331IndexStepRestartAfterDropTest extends TestHelper {

  @Test
  void anIndexStepRestartedAfterItsIndexWasDroppedAsksForANewPlan() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
    database.transaction(() -> database.newDocument("Person").set("id", 7L).save());

    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE id = 7")) {
      final FetchFromIndexStep step = findIndexStep(rs.getExecutionPlan().orElseThrow().getSteps());
      assertThat(step).as("the plan reads the index").isNotNull();
      assertThat(step.indexFileIds()).isNotEmpty();

      // What restartLookup does after a commit overlapped the lookup, and the drop that lands right then
      step.reset();
      database.command("sql", "DROP INDEX `Person[id]`");

      // Nothing is left to unpin of a dropped index, and the restarted read asks for a new plan
      assertThat(step.indexFileIds()).isEmpty();
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      assertThatThrownBy(() -> step.syncPull(context, 10)).isInstanceOf(IndexException.class);
    }

    // Planned again, the statement answers by a scan
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE id = 7")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  private static FetchFromIndexStep findIndexStep(final List<ExecutionStep> steps) {
    if (steps == null)
      return null;
    for (final ExecutionStep step : steps) {
      if (step instanceof FetchFromIndexStep indexStep)
        return indexStep;
      final FetchFromIndexStep nested = findIndexStep(step.getSubSteps());
      if (nested != null)
        return nested;
    }
    return null;
  }
}
