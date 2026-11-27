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
package com.arcadedb.query.sql;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code arcadedb.command.timeout} bounds a SQL statement whichever entry point it is issued through: the {@code query()}
 * path has always honoured it, and each overload of the {@code command()} path has to as well.
 * <p>
 * Every statement below needs far longer than the 50 ms it is given (the rejecting filter walks five hops of a graph
 * of out-degree 3 for every vertex, and the {@code MATCH} and {@code TRAVERSE} are sized the same way), so the one
 * thing that can make it stop is the setting. Each failure has to name the setting and the budget, which also tells it
 * apart from a statement that stopped for another reason.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CommandPathTimeoutTest {
  private static final int    NODES            = 10_000;
  private static final long   BUDGET_MS        = 50L;
  private static final String DB_PATH          = "./target/databases/test-command-path-timeout";
  private static final String REJECTING_FILTER = "out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').size() > 1000000";

  private static Database database;

  /** The three overloads a SQL command can be issued through. */
  enum Overload {
    MAP,
    POSITIONAL,
    WITH_VARIABLES
  }

  @BeforeAll
  static void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++)
        vertices[i] = database.newVertex("Node").set("v", i).save();

      for (int i = 0; i < NODES; i++) {
        vertices[i].newEdge("LINK", vertices[(i + 1) % NODES], true, (Object[]) null).save();
        vertices[i].newEdge("LINK", vertices[(i + 7) % NODES], true, (Object[]) null).save();
        vertices[i].newEdge("LINK", vertices[(i + 31) % NODES], true, (Object[]) null).save();
      }
    });
  }

  @AfterAll
  static void teardown() {
    if (database != null)
      database.drop();
  }

  @AfterEach
  void resetTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @ParameterizedTest
  @EnumSource(Overload.class)
  void selectWithARejectingFilterStopsAtTheBudget(final Overload overload) {
    setTimeout();

    assertStopsAtTheBudget(() -> drain(overload, "SELECT FROM Node WHERE " + REJECTING_FILTER));
  }

  @ParameterizedTest
  @EnumSource(Overload.class)
  void matchStopsAtTheBudget(final Overload overload) {
    setTimeout();

    assertStopsAtTheBudget(() -> drain(overload,
        "MATCH {type: Node, as: a}-LINK->{as: b}-LINK->{as: c}-LINK->{as: d}-LINK->{as: e} RETURN a, e"));
  }

  @ParameterizedTest
  @EnumSource(Overload.class)
  void traverseStopsAtTheBudget(final Overload overload) {
    setTimeout();

    assertStopsAtTheBudget(() -> drain(overload,
        "TRAVERSE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK') FROM Node WHILE $depth < 12"));
  }

  @ParameterizedTest
  @EnumSource(Overload.class)
  void updateStopsAtTheBudget(final Overload overload) {
    setTimeout();

    assertStopsAtTheBudget(() -> database.transaction(
        () -> drain(overload, "UPDATE Node SET touched = true WHERE " + REJECTING_FILTER)));
  }

  @ParameterizedTest
  @EnumSource(Overload.class)
  void deleteStopsAtTheBudget(final Overload overload) {
    setTimeout();

    assertStopsAtTheBudget(() -> database.transaction(
        () -> drain(overload, "DELETE FROM Node WHERE " + REJECTING_FILTER)));
  }

  @Test
  void aSubqueryDoesNotRestartTheBudget() {
    setTimeout();

    assertStopsAtTheBudget(() -> drain(Overload.MAP,
        "SELECT FROM (SELECT FROM Node WHERE " + REJECTING_FILTER + ")"));
  }

  @Test
  void openCypherCommandStopsAtTheBudget() {
    setTimeout();

    assertStopsAtTheBudget(() -> {
      try (final ResultSet resultSet = database.command("opencypher",
          "MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN a.v, b.v")) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    });
  }

  @Test
  void theDisabledDefaultLeavesCommandsAlone() {
    assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT)).isZero();

    try (final ResultSet resultSet = database.command("sql", "SELECT count(*) AS c FROM Node WHERE v >= 0")) {
      assertThat(resultSet.next().<Long>getProperty("c")).isEqualTo((long) NODES);
    }
  }

  @Test
  void aStatementThatFinishesInsideTheBudgetIsNotInterrupted() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

    try (final ResultSet resultSet = database.command("sql", "SELECT count(*) AS c FROM Node WHERE v >= 0")) {
      assertThat(resultSet.next().<Long>getProperty("c")).isEqualTo((long) NODES);
    }
  }

  /**
   * The same rule one level down: a context that knows its database resolves the budget from it even when the contexts it
   * hangs from know none, whichever of them was asked first.
   */
  @Nested
  class ContextWithoutDatabaseAbove {
    @Test
    void theBudgetIsReadFromTheDatabaseOfTheContextThatHasOne() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final BasicCommandContext above = new BasicCommandContext();
      final BasicCommandContext statement = statementUnder(above);

      assertThat(statement.getCommandTimeout()).isEqualTo(60_000L);
      assertThat(statement.getCommandDeadline()).isNotEqualTo(Long.MAX_VALUE);
      assertThat(statement.getCommandDeadlineDescription()).isEqualTo(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 60000ms");
    }

    @Test
    void theContextAboveBeingAskedFirstChangesNothing() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final BasicCommandContext above = new BasicCommandContext();
      final BasicCommandContext statement = statementUnder(above);
      assertThat(above.getCommandDeadline()).as("it knows no database, so it knows no budget").isEqualTo(Long.MAX_VALUE);

      assertThat(statement.getCommandTimeout()).isEqualTo(60_000L);
      assertThat(statement.getCommandDeadline()).isNotEqualTo(Long.MAX_VALUE);
    }

    @Test
    void theDeadlineIsTakenOnceAndNestedContextsShareIt() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final BasicCommandContext statement = statementUnder(new BasicCommandContext());
      final long deadline = statement.getCommandDeadline();

      assertThat(statement.getCommandDeadline()).isEqualTo(deadline);
      final BasicCommandContext nested = new BasicCommandContext();
      nested.setParentWithoutOverridingChild(statement);
      assertThat(nested.getCommandDeadline()).isEqualTo(deadline);
      assertThat(nested.getCommandDeadlineDescription()).isEqualTo(statement.getCommandDeadlineDescription());
      assertThat(statement.copy().getCommandDeadline()).isEqualTo(deadline);
    }

    @Test
    void theRegexBoundIsReadFromTheDatabaseOfTheContextThatHasOne() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 60_000L);
      try {
        final BasicCommandContext statement = statementUnder(new BasicCommandContext());

        final long remainingNanos = statement.getRegexDeadline() - System.nanoTime();
        assertThat(remainingNanos).isBetween(30_000L * 1_000_000L, 60_000L * 1_000_000L);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT,
            GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getDefValue());
      }
    }

    @Test
    void aChainWithoutAnyDatabaseStaysUnbounded() {
      final BasicCommandContext above = new BasicCommandContext();
      final BasicCommandContext below = new BasicCommandContext();
      below.setParentWithoutOverridingChild(above);

      assertThat(below.getCommandTimeout()).isZero();
      assertThat(below.getCommandDeadline()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void aDeadlinePinnedAboveIsInheritedWhateverTheDatabaseSays() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final BasicCommandContext above = new BasicCommandContext();
      above.setCommandDeadline(0L, "TIMEOUT clause of 50ms", true);
      final BasicCommandContext statement = statementUnder(above);

      assertThat(statement.getCommandDeadline()).isZero();
      assertThat(statement.isCommandDeadlinePartial()).isTrue();
      assertThat(statement.getCommandDeadlineDescription()).isEqualTo("TIMEOUT clause of 50ms");
    }

    @Test
    void theBudgetAHostForwardedWinsOverTheDatabase() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);
      CommandTimeoutOverride.set(1_000L);
      try {
        final BasicCommandContext statement = statementUnder(new BasicCommandContext());

        assertThat(statement.getCommandTimeout()).isEqualTo(1_000L);
      } finally {
        CommandTimeoutOverride.clear();
      }
    }

    private BasicCommandContext statementUnder(final CommandContext above) {
      final BasicCommandContext statement = new BasicCommandContext();
      statement.setParentWithoutOverridingChild(above);
      statement.setDatabase((DatabaseInternal) database);
      return statement;
    }
  }

  private static void setTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, BUDGET_MS);
  }

  private static void assertStopsAtTheBudget(final ThrowingCallable statement) {
    assertThatThrownBy(statement)
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of " + BUDGET_MS + "ms");
  }

  private static void drain(final Overload overload, final String query) {
    try (final ResultSet resultSet = execute(overload, query)) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private static ResultSet execute(final Overload overload, final String query) {
    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    return switch (overload) {
      case MAP -> database.command("sql", query, new HashMap<String, Object>());
      case POSITIONAL -> database.command("sql", query, new Object[0]);
      case WITH_VARIABLES -> engine.command(query, new ContextConfiguration(), new HashMap<String, Object>(),
          Map.<String, Object>of("marker", 1));
    };
  }
}
