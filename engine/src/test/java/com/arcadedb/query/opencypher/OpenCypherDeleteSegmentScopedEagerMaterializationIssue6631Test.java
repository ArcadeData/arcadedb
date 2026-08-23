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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.ForeachStep;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6631: the eager-materialization gate that {@code DeleteStep}/{@code
 * ForeachStep} use to guard against issue #6491 (a disconnected-pattern MATCH re-observing an
 * already-deleted entity across output rows) was scoped to the whole statement rather than to the MATCH
 * clause(s) actually feeding a given DELETE/FOREACH segment. A multi-segment statement (WITH-separated
 * MATCH/write pairs) where any segment happens to contain an unrelated, incidental disconnected-pattern
 * MATCH forced every DELETE/FOREACH in the statement onto the eager, whole-row-set-buffered path, even a
 * bulk delete elsewhere that has nothing to do with the disconnected pattern.
 * <p>
 * These tests read the private {@code eagerMaterialize} field off the built {@code DeleteStep}/{@code
 * ForeachStep} via reflection - the flag has no other externally observable effect short of a memory
 * measurement, since both the eager and the streaming path delete the same rows and return the same
 * results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OpenCypherDeleteSegmentScopedEagerMaterializationIssue6631Test {
  private Database database;

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isOpen())
        database.drop();
      database = null;
    }
  }

  private static boolean eagerMaterializeOf(final Object step) {
    try {
      final Field field = step.getClass().getDeclaredField("eagerMaterialize");
      field.setAccessible(true);
      return field.getBoolean(step);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <T> T findStep(final ResultSet result, final Class<T> type) {
    final Optional<ExecutionPlan> plan = result.getExecutionPlan();
    assertThat(plan).isPresent();
    for (final ExecutionStep step : plan.get().getSteps())
      if (type.isInstance(step))
        return type.cast(step);
    throw new IllegalStateException("No " + type.getSimpleName() + " found in execution plan");
  }

  @Test
  void deleteUnrelatedToAnEarlierSegmentsDisconnectedMatchIsNotEagerlyMaterialized() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-unrelated").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag {name: 'x'})");
      database.command("opencypher", "CREATE (:Tag {name: 'y'})");
      database.command("opencypher", "CREATE (:Big {id: 1})");
      database.command("opencypher", "CREATE (:Big {id: 2})");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (a:Tag {name: 'x'}), (b:Tag {name: 'y'}) WITH a, b "
              + "MATCH (n:Big) DETACH DELETE n")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("DELETE fed only by a connected MATCH must not pay the eager-materialization "
                + "cost just because an earlier, unrelated WITH-separated segment has a "
                + "disconnected-pattern MATCH")
            .isFalse();
      }
    });

    try (ResultSet remaining = database.query("opencypher", "MATCH (n:Big) RETURN n")) {
      assertThat(remaining.hasNext()).isFalse();
    }
  }

  @Test
  void deleteDirectlyFedByADisconnectedMatchStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-direct").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Loop {tag: null})");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
      database.command("opencypher", "MATCH (n:Loop) CREATE (n)-[:SELF]->(n)");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (o:Other), (n:Loop)<-[:SELF]-(n) WHERE n.tag IS NULL "
              + "DETACH DELETE n RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("DELETE fed by a disconnected-pattern MATCH in its own segment must keep "
                + "eagerly materializing - regression guard for issue #6491")
            .isTrue();
      }
    });
  }

  @Test
  void foreachDeleteUnrelatedToAnEarlierSegmentsDisconnectedMatchIsNotEagerlyMaterialized() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-foreach-unrelated").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag {name: 'x'})");
      database.command("opencypher", "CREATE (:Tag {name: 'y'})");
      database.command("opencypher", "CREATE (:Big {id: 1})");
      database.command("opencypher", "CREATE (:Big {id: 2})");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (a:Tag {name: 'x'}), (b:Tag {name: 'y'}) WITH a, b "
              + "MATCH (n:Big) FOREACH (x IN [1] | DETACH DELETE n)")) {
        while (result.hasNext())
          result.next();

        final ForeachStep foreachStep = findStep(result, ForeachStep.class);
        assertThat(eagerMaterializeOf(foreachStep))
            .withFailMessage("FOREACH DELETE fed only by a connected MATCH must not pay the "
                + "eager-materialization cost just because an earlier, unrelated WITH-separated segment "
                + "has a disconnected-pattern MATCH")
            .isFalse();
      }
    });

    try (ResultSet remaining = database.query("opencypher", "MATCH (n:Big) RETURN n")) {
      assertThat(remaining.hasNext()).isFalse();
    }
  }

  @Test
  void foreachDeleteDirectlyFedByADisconnectedMatchStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-foreach-direct").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Loop {tag: null})");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
      database.command("opencypher", "MATCH (n:Loop) CREATE (n)-[:SELF]->(n)");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (o:Other), (n:Loop)<-[:SELF]-(n) WHERE n.tag IS NULL "
              + "FOREACH (x IN [1] | DETACH DELETE n) RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final ForeachStep foreachStep = findStep(result, ForeachStep.class);
        assertThat(eagerMaterializeOf(foreachStep))
            .withFailMessage("FOREACH DELETE fed by a disconnected-pattern MATCH in its own segment must "
                + "keep eagerly materializing - regression guard for issue #6491")
            .isTrue();
      }
    });
  }
}
