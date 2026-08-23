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
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.opencypher.executor.steps.ForeachStep;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
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
 * The straightforward fix - clear the tracked MATCH clauses at every WITH boundary - has its own gap:
 * a WITH that plainly forwards a variable bound by a disconnected-pattern MATCH (e.g. {@code WITH n, o})
 * does not resolve the #6491 hazard for that variable, since rows out of a disconnected-pattern MATCH
 * still flow one at a time through a non-aggregating WITH. {@code CypherExecutionPlan} guards against
 * this with a persistent "tainted variable" set: once a segment is found disconnected, its variables stay
 * tainted for the rest of the statement, and a later DELETE/FOREACH is still eagerly materialized if it
 * targets one of them - regardless of how many WITH clauses forwarded it in between.
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
  void deleteOfADisconnectedMatchsOwnVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-passthrough").create();

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
              + "WITH n, o "
              + "DETACH DELETE n RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("A WITH that plainly forwards a disconnected-pattern MATCH's own variable "
                + "(WITH n, o) does not neutralize the issue #6491 hazard for that variable - a DELETE of "
                + "it downstream of the WITH must still eagerly materialize")
            .isTrue();
      }
    });
  }

  /**
   * Same hazard as {@link #deleteOfADisconnectedMatchsOwnVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes()},
   * but the forwarded, tainted variable is a relationship bound by the disconnected MATCH rather than a
   * node - {@code closeMatchSegment()} must taint relationship variables too, not just node variables,
   * since a disconnected MATCH can rebind the same underlying edge across rows exactly as it can a
   * vertex (see {@code DeleteStep}'s own {@code eagerMaterialize} field doc).
   */
  @Test
  void deleteOfADisconnectedMatchsOwnRelationshipVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-rel-passthrough").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:A)-[:REL]->(:B)");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (a:A)-[r:REL]->(b:B), (o:Other) "
              + "WITH r, o "
              + "DELETE r RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("A WITH that plainly forwards a disconnected-pattern MATCH's own relationship "
                + "variable (WITH r, o) does not neutralize the issue #6491 hazard for that variable - a "
                + "DELETE of it downstream of the WITH must still eagerly materialize")
            .isTrue();
      }
    });

    try (ResultSet remaining = database.query("opencypher", "MATCH ()-[r:REL]->() RETURN r")) {
      assertThat(remaining.hasNext()).isFalse();
    }
  }

  /**
   * Same hazard as {@link #deleteOfADisconnectedMatchsOwnVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes()},
   * but the passthrough is spelled {@code WITH *} instead of naming the forwarded variables. {@code
   * propagateTaintThroughRenames()} explicitly skips star items (there is no single expression/alias pair
   * to inspect), so this pins down that the direct, same-name taint {@code closeMatchSegment()} already
   * applies is what covers this shape - not the rename-propagation logic.
   */
  @Test
  void deleteOfADisconnectedMatchsOwnVariableForwardedThroughAStarWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-star").create();

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
              + "WITH * "
              + "DETACH DELETE n RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("A WITH * that forwards everything unchanged does not neutralize the issue "
                + "#6491 hazard for a disconnected-pattern MATCH's own variable - a DELETE of it "
                + "downstream of the WITH must still eagerly materialize")
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

  @Test
  void foreachDeleteOfADisconnectedMatchsOwnVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-foreach-passthrough").create();

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
              + "WITH n, o "
              + "FOREACH (x IN [1] | DETACH DELETE n) RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final ForeachStep foreachStep = findStep(result, ForeachStep.class);
        assertThat(eagerMaterializeOf(foreachStep))
            .withFailMessage("A WITH that plainly forwards a disconnected-pattern MATCH's own variable "
                + "(WITH n, o) does not neutralize the issue #6491 hazard for that variable - a FOREACH "
                + "DELETE of it downstream of the WITH must still eagerly materialize")
            .isTrue();
      }
    });
  }

  /**
   * Same hazard as {@link #deleteOfADisconnectedMatchsOwnVariableForwardedThroughAPassthroughWithStillEagerlyMaterializes()},
   * but the WITH renames the tainted variable ({@code WITH n AS m}) instead of forwarding it under the
   * same name - a rename doesn't change how rows flow either, so it must not lose the taint. Without
   * {@code propagateTaintThroughRenames()}, {@code closeMatchSegment()} taints {@code n} but a later
   * {@code DELETE m} checks {@code m} against the taint set and finds nothing.
   */
  @Test
  void deleteOfADisconnectedMatchsOwnVariableRenamedThroughWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-rename").create();

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
              + "WITH n AS m, o "
              + "DETACH DELETE m RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("A WITH that renames a disconnected-pattern MATCH's own variable "
                + "(WITH n AS m) does not neutralize the issue #6491 hazard for it - a DELETE of the new "
                + "name downstream of the WITH must still eagerly materialize")
            .isTrue();
      }
    });
  }

  /** Same as above, but renaming the disconnected MATCH's relationship variable rather than a node. */
  @Test
  void deleteOfADisconnectedMatchsOwnRelationshipVariableRenamedThroughWithStillEagerlyMaterializes() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6631-delete-rel-rename").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:A)-[:REL]->(:B)");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
    });

    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "PROFILE MATCH (a:A)-[r:REL]->(b:B), (o:Other) "
              + "WITH r AS r2, o "
              + "DELETE r2 RETURN o.id AS id")) {
        while (result.hasNext())
          result.next();

        final DeleteStep deleteStep = findStep(result, DeleteStep.class);
        assertThat(eagerMaterializeOf(deleteStep))
            .withFailMessage("A WITH that renames a disconnected-pattern MATCH's own relationship "
                + "variable (WITH r AS r2) does not neutralize the issue #6491 hazard for it - a DELETE "
                + "of the new name downstream of the WITH must still eagerly materialize")
            .isTrue();
      }
    });

    try (ResultSet remaining = database.query("opencypher", "MATCH ()-[r:REL]->() RETURN r")) {
      assertThat(remaining.hasNext()).isFalse();
    }
  }

  /**
   * Backs the comment on {@code CypherExecutionPlan.buildExecutionStepsLegacy()}'s DELETE construction
   * site, which leaves it using the unscoped, statement-wide {@code matchClausesHaveDisconnectedPatterns
   * (statement.getMatchClauses())} rather than this issue's segment-scoped fix: that is only safe if a
   * multi-segment, WITH-separated MATCH/DELETE statement - the shape #6631 is about - can never actually
   * reach that method, because {@code CypherExecutionPlan.buildExecutionSteps()} only falls back to it
   * when {@code statement.getClausesInOrder()} is null or empty. Parses the same query shape used by
   * {@link #deleteUnrelatedToAnEarlierSegmentsDisconnectedMatchIsNotEagerlyMaterialized()} through the
   * real production parser and asserts {@code getClausesInOrder()} is populated, confirming the premise
   * with an executable check rather than an unverified comment.
   */
  @Test
  void aMultiSegmentWithSeparatedStatementAlwaysPopulatesClausesInOrder() {
    final CypherStatement statement = new Cypher25AntlrParser().parse(
        "MATCH (a:Tag {name: 'x'}), (b:Tag {name: 'y'}) WITH a, b MATCH (n:Big) DETACH DELETE n");

    assertThat(statement.getClausesInOrder())
        .withFailMessage("A multi-segment, WITH-separated MATCH/DELETE statement must populate "
            + "getClausesInOrder() when parsed through the real parser - otherwise "
            + "buildExecutionStepsLegacy()'s unscoped DELETE construction site would be reachable for "
            + "exactly the shape issue #6631 is about")
        .isNotNull()
        .isNotEmpty();
  }
}
