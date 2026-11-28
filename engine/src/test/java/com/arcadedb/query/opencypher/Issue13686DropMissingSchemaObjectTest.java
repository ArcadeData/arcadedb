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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.ConstraintDropFailedException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.IndexDropFailedException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13686: a {@code DROP INDEX} or {@code DROP CONSTRAINT} of a name nothing holds raised a plain {@code SchemaException},
 * which the Bolt protocol can only answer as a semantic error, where Neo4j answers {@code IndexDropFailed} /
 * {@code ConstraintDropFailed}. The refusal says which drop failed, in Neo4j's words, and stays a schema error for every other
 * protocol. The name a {@code $parameter} gives is the one the statement drops, and the one a refusal names.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13686DropMissingSchemaObjectTest extends TestHelper {

  @Test
  void dropOfAMissingIndexIsAFailedIndexDrop() {
    final Throwable refused = catchThrowable(() -> command("DROP INDEX vector", Map.of()));
    final IndexDropFailedException failed = CauseChain.find(refused, IndexDropFailedException.class);
    assertThat(failed).as("the refusal of DROP INDEX vector").isNotNull();
    assertThat(failed.getMessage()).isEqualTo("Unable to drop index called `vector`. There is no such index.");
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.SCHEMA);
  }

  @Test
  void dropOfAMissingConstraintIsAFailedConstraintDrop() {
    final Throwable refused = catchThrowable(() -> command("DROP CONSTRAINT nothing", Map.of()));
    final ConstraintDropFailedException failed = CauseChain.find(refused, ConstraintDropFailedException.class);
    assertThat(failed).as("the refusal of DROP CONSTRAINT nothing").isNotNull();
    assertThat(failed.getMessage()).isEqualTo("Unable to drop constraint `nothing`: No such constraint nothing.");
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.SCHEMA);
  }

  @Test
  void aPlainIndexNameIsNoConstraintToDrop() {
    command("CREATE INDEX plain13686 FOR (n:A13686) ON (n.p)", Map.of());
    final Throwable refused = catchThrowable(() -> command("DROP CONSTRAINT plain13686", Map.of()));
    assertThat(CauseChain.find(refused, ConstraintDropFailedException.class)).isNotNull();
    assertThat(database.getSchema().existsIndex("plain13686")).isTrue();
  }

  @Test
  void dropOfAnIndexAConstraintOwnsIsAFailedIndexDrop() {
    command("CREATE CONSTRAINT c13686 FOR (n:U13686) REQUIRE n.x IS UNIQUE", Map.of());
    final String indexName = database.getSchema().getType("U13686").getPolymorphicIndexByProperties("x").getName();
    final Throwable refused = catchThrowable(() -> command("DROP INDEX `" + indexName + "`", Map.of()));
    final IndexDropFailedException failed = CauseChain.find(refused, IndexDropFailedException.class);
    assertThat(failed).isNotNull();
    assertThat(failed.getMessage()).contains("c13686");
    assertThat(database.getSchema().existsIndex(indexName)).isTrue();
  }

  @Test
  void createDropAndDropAgain() {
    command("CREATE INDEX vector FOR (n:A13686) ON (n.p)", Map.of());
    assertThat(stats("DROP INDEX vector", Map.of()).getIndexesRemoved()).isEqualTo(1);
    assertThat(CauseChain.find(catchThrowable(() -> command("DROP INDEX vector", Map.of())), IndexDropFailedException.class)).isNotNull();
  }

  @Test
  void ifExistsStillDropsNothingQuietly() {
    assertThat(stats("DROP INDEX vector IF EXISTS", Map.of()).getIndexesRemoved()).isZero();
    assertThat(stats("DROP CONSTRAINT nothing IF EXISTS", Map.of()).getConstraintsRemoved()).isZero();
  }

  @Test
  void theNameAParameterGivesIsTheOneDropped() {
    command("CREATE INDEX byparam13686 FOR (n:A13686) ON (n.p)", Map.of());
    assertThat(stats("DROP INDEX $n", Map.of("n", "byparam13686")).getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("byparam13686")).isFalse();

    command("CREATE CONSTRAINT cparam13686 FOR (n:U13686) REQUIRE n.y IS UNIQUE", Map.of());
    assertThat(stats("DROP CONSTRAINT $c", Map.of("c", "cparam13686")).getConstraintsRemoved()).isEqualTo(1);
  }

  @Test
  void theNameAParameterGivesIsTheOneCreated() {
    command("CREATE INDEX $n FOR (n:A13686) ON (n.q)", Map.of("n", "created13686"));
    assertThat(database.getSchema().existsIndex("created13686")).isTrue();
    assertThat(database.getSchema().existsIndex("$n")).isFalse();

    command("CREATE CONSTRAINT $c FOR (n:U13686) REQUIRE n.z IS UNIQUE", Map.of("c", "cz13686"));
    assertThat(stats("DROP CONSTRAINT cz13686", Map.of()).getConstraintsRemoved()).isEqualTo(1);
  }

  @Test
  void aNameParameterThatIsNoStringIsRefused() {
    final Throwable refused = catchThrowable(() -> command("DROP INDEX $n", Map.of("n", 5)));
    assertThat(CauseChain.find(refused, TypeMismatchException.class)).isNotNull();
    assertThat(CauseChain.find(refused, TypeMismatchException.class).getMessage()).contains("$n").contains("String");
  }

  @Test
  void aMissingNameAParameterGivesIsTheOneTheRefusalNames() {
    final IndexDropFailedException index = CauseChain.find(catchThrowable(() -> command("DROP INDEX $n", Map.of("n", "vector"))),
        IndexDropFailedException.class);
    assertThat(index).isNotNull();
    assertThat(index.getMessage()).isEqualTo("Unable to drop index called `vector`. There is no such index.");

    final ConstraintDropFailedException constraint = CauseChain.find(
        catchThrowable(() -> command("DROP CONSTRAINT $c", Map.of("c", "nothing"))), ConstraintDropFailedException.class);
    assertThat(constraint).isNotNull();
    assertThat(constraint.getMessage()).isEqualTo("Unable to drop constraint `nothing`: No such constraint nothing.");
  }

  private void command(final String statement, final Map<String, Object> parameters) {
    database.transaction(() -> {
      try (final ResultSet ignored = database.command("opencypher", statement, parameters)) {
        // DDL returns no rows
      }
    });
  }

  private QueryStatistics stats(final String statement, final Map<String, Object> parameters) {
    final QueryStatistics[] stats = new QueryStatistics[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, parameters)) {
        stats[0] = rs.getStatistics().orElseThrow();
      }
    });
    return stats[0];
  }
}
