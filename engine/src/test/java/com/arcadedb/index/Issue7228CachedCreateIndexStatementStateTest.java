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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A parsed statement is a CACHED, SHARED object: {@code StatementCache.get} hands the same instance back for the same
 * text, execution after execution, and the {@code sql} engine goes through it on every command.
 * {@code CreateIndexStatement} mutates two of its own fields while it runs - a {@code $variable} type name is replaced
 * by what it resolved to, and an unnamed statement has {@code name} backfilled with the auto-derived
 * {@code typeName[properties]} form - and used to restore only the first, only on the success path.
 * <p>
 * Every {@code return} and {@code throw} before that line therefore leaked the resolved type into the shared
 * statement, the {@code IF NOT EXISTS} shortcut chief among them. The next execution of the same text then saw a type
 * name that no longer starts with {@code $}, skipped variable resolution entirely, and indexed the FIRST execution's
 * type - silently, with no error to say the variable had been ignored.
 * <p>
 * The {@code $variable} + cache combination is reached in production through
 * {@code SQLQueryEngine.command(query, configuration, parameters, variables)}, which is what
 * {@code SQLTriggerExecutor} runs a trigger body with; the tests use the same entry point. ({@code sqlscript} cannot
 * expose the defect: it has no statement cache and re-parses every execution.)
 * <p>
 * Found while reviewing the #7228 fix, which adds one more early return through the same shortcut.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7228CachedCreateIndexStatementStateTest extends TestHelper {

  /** Identical text on every execution, so all of them go through ONE cached statement instance. */
  private static final String STATEMENT = "CREATE INDEX IF NOT EXISTS ON $t (uuid) UNIQUE";

  @Override
  public void beginTest() {
    database.transaction(() -> {
      for (final String type : new String[] { "TypeA", "TypeB" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".uuid STRING");
      }
    });
  }

  @Test
  void aGuardedStatementOverAVariableTypeDoesNotCarryTheResolvedTypeIntoTheNextExecution() {
    // 1st: creates the index on TypeA and leaves through the success path.
    run("TypeA");
    // 2nd: same type, so the index already exists and the statement leaves through the IF NOT EXISTS shortcut - the
    // early return that used to skip the restore.
    run("TypeA");
    // 3rd: a DIFFERENT type. With the leak $t is never re-resolved and this indexes TypeA all over again.
    run("TypeB");

    assertThat(database.getSchema().getType("TypeB").getAllIndexes(false))
        .as("the third execution asked for an index on TypeB, so TypeB must have one")
        .isNotEmpty();
    assertThat(database.getSchema().existsIndex("TypeB[uuid]")).isTrue();
    assertThat(database.getSchema().getIndexByName("TypeB[uuid]").getTypeName()).isEqualTo("TypeB");
  }

  /** The constraint really landed on TypeB, so the statement did what it reported. */
  @Test
  void theInheritedConstraintIsEnforcedOnTheTypeTheLastExecutionNamed() {
    run("TypeA");
    run("TypeA");
    run("TypeB");

    database.transaction(() -> database.command("sql", "INSERT INTO TypeB SET uuid = 'u1'"));

    assertThat(database.getSchema().getIndexByName("TypeB[uuid]").isUnique()).isTrue();
  }

  /**
   * Straight at the shared object: after any execution the statement must read back exactly as it was parsed, so the
   * next caller of the same text gets the statement they wrote and not the previous execution's residue.
   */
  @Test
  void theCachedStatementIsUnchangedByExecution() {
    run("TypeA");

    final Statement cached = ((DatabaseInternal) database).getStatementCache().get(STATEMENT);
    assertThat(cached).isInstanceOf(CreateIndexStatement.class);

    final CreateIndexStatement createIndex = (CreateIndexStatement) cached;
    assertThat(createIndex.typeName.getStringValue())
        .as("the type name must still be the variable the statement was written with")
        .isEqualTo("$t");
    assertThat(createIndex.name)
        .as("the statement named no index, so no auto-derived name may survive its execution")
        .isNull();

    // ...and still true after the guarded execution that returns through the shortcut.
    run("TypeA");
    assertThat(((CreateIndexStatement) ((DatabaseInternal) database).getStatementCache().get(STATEMENT)).typeName.getStringValue())
        .isEqualTo("$t");
  }

  private void run(final String type) {
    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    try (final ResultSet rs = engine.command(STATEMENT, database.getConfiguration(), Map.of(), Map.of("$t", type))) {
      rs.stream().forEach(r -> {
      });
    }
  }
}
