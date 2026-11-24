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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.query.ShowCommandTail;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code YIELD}/{@code WHERE} tail of a {@code SHOW} command is the clause the {@code SHOW} grammar defines - {@code YIELD},
 * its {@code ORDER BY}/{@code SKIP}/{@code LIMIT}/{@code WHERE}, then a {@code RETURN} - and what it produces is bounded by the
 * element cap and the heap budget of the queries, like the rows any other operation buffers. A command that is not a
 * {@code SHOW} ({@code CALL dbms.components() YIELD ...}) keeps the clauses a {@code CALL} may be followed by, under the same bounds.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ShowCommandTailBoundsTest extends TestHelper {
  private static final List<String> DATABASE_FIELDS = List.of("name", "type", "access");

  private static final List<List<Object>> DATABASE_ROWS = List.of(
      List.of("alpha", "standard", "read-write"),
      List.of("beta", "standard", "read-only"),
      List.of("system", "system", "read-write"));

  private static final List<String> COMPONENT_FIELDS = List.of("name", "versions", "edition");

  private static final List<List<Object>> COMPONENT_ROWS = List.of(
      List.of("Neo4j Kernel", List.of("5.26.0", "5.26.1"), "community"));

  @BeforeEach
  void noReservationLeft() {
    // What queries of other tests abandoned is given back at any moment until then: the reservation read below is this test's own
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
  }

  @AfterEach
  void resetSettings() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.reset();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SHOW DATABASES YIELD * UNWIND range(1, 3) AS x RETURN x",
      "SHOW DATABASES YIELD name WITH name RETURN name",
      "SHOW DATABASES YIELD name MATCH (n) RETURN n",
      "SHOW DATABASES YIELD name, type UNWIND range(1, 3) AS a UNWIND range(1, 3) AS b RETURN name, a, b",
      "SHOW DATABASES WHERE name = 'beta' UNWIND [1, 2] AS x RETURN x",
      "SHOW DATABASES YIELD name CALL { UNWIND range(1, 3) AS x RETURN x } RETURN name, x",
      "SHOW DATABASES YIELD name ORDER BY name LIMIT 2 UNWIND range(1, 3) AS x RETURN x",
  })
  void aShowTailMadeOfOtherClausesIsRefused(final String query) {
    assertThatThrownBy(() -> ShowCommandTail.apply(database, query, DATABASE_FIELDS, DATABASE_ROWS, Map.of()))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void aTerminateTailMadeOfOtherClausesIsRefused() {
    assertThatThrownBy(() -> ShowCommandTail.apply(database, "TERMINATE TRANSACTIONS 'a' YIELD message UNWIND range(1, 3) AS x RETURN x",
        List.of("transactionId", "username", "message"), List.of(List.of("a", "root", "Transaction terminated.")), Map.of()))
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void aShowTailKeepsTheClausesOfItsGrammar() {
    assertThat(apply("SHOW DATABASES YIELD name, type WHERE type = 'standard' RETURN name ORDER BY name DESC LIMIT 5").rows())
        .hasSize(2);
    assertThat(apply("SHOW DATABASES YIELD name AS db ORDER BY db SKIP 1 LIMIT 1 WHERE db <> 'zzz'").rows()).hasSize(1);
    assertThat(apply("SHOW DATABASES YIELD * WHERE access = 'read-write' AND name IN ['alpha', 'system']").rows()).hasSize(2);
    assertThat(apply("SHOW DATABASES WHERE name STARTS WITH 'al' OR name ENDS WITH 'ta'").rows()).hasSize(2);
    assertThat(apply("SHOW DATABASES YIELD name, type RETURN DISTINCT type").rows()).hasSize(2);
    assertThat(apply("SHOW DATABASES YIELD name RETURN count(name) AS total;").rows()).hasSize(1);
    // A keyword inside a bracket belongs to the expression it is written in
    assertThat(apply("SHOW DATABASES WHERE name IN [x IN range(0, 3) WHERE x > 5 | 'beta'] OR name = 'alpha' OR name = 'system'")
        .rows()).hasSize(2);
  }

  /**
   * A command that is not a SHOW is a CALL, which a query may follow with any of its clauses: Neo4j's own documentation of
   * {@code dbms.components} unwinds the versions it yields.
   */
  @Test
  void aCallTailKeepsTheClausesAFollowingQueryMayHold() {
    final ShowCommandTail.Table table = ShowCommandTail.apply(database,
        "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN name, version, edition",
        COMPONENT_FIELDS, COMPONENT_ROWS, Map.of());

    assertThat(table.fields()).containsExactly("name", "version", "edition");
    assertThat(table.rows()).hasSize(2);
    assertThat(table.rows().getFirst()).containsExactly("Neo4j Kernel", "5.26.0", "community");
  }

  @Test
  void theRowsOfATailAreBoundedByTheElementLimit() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);

    assertThat(call("CALL dbms.components() YIELD name UNWIND range(1, 50) AS x RETURN x").rows()).hasSize(50);

    assertThatThrownBy(() -> call("CALL dbms.components() YIELD name UNWIND range(1, 1000) AS x RETURN x"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

    // Nested ranges multiply: each one is short, their product is what is buffered
    assertThatThrownBy(() -> call(
        "CALL dbms.components() YIELD name UNWIND range(1, 40) AS a UNWIND range(1, 40) AS b RETURN a, b"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  @Test
  void theRowsOfATailAreChargedToTheHeapBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1);

    assertThatThrownBy(() -> call("CALL dbms.components() YIELD name UNWIND range(1, 100000) AS x RETURN x, toString(x) AS s"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).as("a refused tail gave its heap back").isZero();

    // What fits is answered, and the table handed over holds no reservation
    assertThat(call("CALL dbms.components() YIELD name UNWIND range(1, 100) AS x RETURN x").rows()).hasSize(100);
    assertThat(QueryHeapBudget.getReservedBytes()).as("the table handed over holds no reservation").isZero();
  }

  @Test
  void aCommandWithNothingToApplyAnswersItsTableUnchanged() {
    assertThat(ShowCommandTail.hasTail(null)).isFalse();
    assertThat(ShowCommandTail.hasTail("SHOW DATABASES")).isFalse();
    assertThat(ShowCommandTail.hasTail("SHOW DATABASES WHERE name = 'beta'")).isTrue();

    final ShowCommandTail.Table noDatabase = ShowCommandTail.apply(null, "SHOW DATABASES WHERE name = 'beta'", DATABASE_FIELDS,
        DATABASE_ROWS, Map.of());
    assertThat(noDatabase.fields()).isSameAs(DATABASE_FIELDS);
    assertThat(noDatabase.rows()).isSameAs(DATABASE_ROWS);

    final ShowCommandTail.Table noQuery = ShowCommandTail.apply(database, null, DATABASE_FIELDS, DATABASE_ROWS, Map.of());
    assertThat(noQuery.fields()).isSameAs(DATABASE_FIELDS);
    assertThat(noQuery.rows()).isSameAs(DATABASE_ROWS);

    final ShowCommandTail.Table noTail = apply("SHOW DATABASES");
    assertThat(noTail.fields()).isSameAs(DATABASE_FIELDS);
    assertThat(noTail.rows()).isSameAs(DATABASE_ROWS);
  }

  /** A handle that is not the engine's own database, as a client of the engine may hold, answers its tail all the same. */
  @Test
  void aTailIsAnsweredForADatabaseHandleThatIsNotTheEnginesOwn() {
    final BasicDatabase handle = handleOf(database);
    assertThat(handle).isNotInstanceOf(DatabaseInternal.class).isNotInstanceOf(Database.class);

    assertThat(ShowCommandTail.apply(handle, "SHOW DATABASES YIELD name, type WHERE type = 'standard' RETURN name", DATABASE_FIELDS,
        DATABASE_ROWS, Map.of()).rows()).hasSize(2);

    // The element cap applies to it too, read from the settings of the server as the handle gives no database to read them from
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(100L);

    assertThat(ShowCommandTail.apply(handle, "CALL dbms.components() YIELD name UNWIND range(1, 50) AS x RETURN x", COMPONENT_FIELDS,
        COMPONENT_ROWS, Map.of()).rows()).hasSize(50);

    assertThatThrownBy(() -> ShowCommandTail.apply(handle, "CALL dbms.components() YIELD name UNWIND range(1, 1000) AS x RETURN x",
        COMPONENT_FIELDS, COMPONENT_ROWS, Map.of()))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  /** A clause nested past what the stack of the thread can hold is a parse error of the client, never a raw stack overflow. */
  @Test
  void aTailNestedPastTheStackIsRefusedAsAParseError() throws InterruptedException {
    // Past what the guard of the nesting depth would stop first, so it is the stack that gives out
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH, 1_000_000);

    final String tail = "WHERE " + "(".repeat(20_000) + "true" + ")".repeat(20_000);
    final Cypher25AntlrParser parser = new Cypher25AntlrParser(database);
    final AtomicReference<Throwable> outcome = new AtomicReference<>();

    final Thread thread = new Thread(null, () -> {
      try {
        parser.checkShowCommandYield(tail);
      } catch (final Throwable e) {
        outcome.set(e);
      }
    }, "show-tail-small-stack", 256 * 1024);
    thread.start();
    thread.join();

    // The message and the cause tell the stack that gave out from the guard of the nesting depth, which also says "too deeply"
    assertThat(outcome.get()).isInstanceOf(CommandParsingException.class).hasMessageContaining("stack exhausted")
        .hasCauseInstanceOf(StackOverflowError.class);
  }

  private static BasicDatabase handleOf(final Database target) {
    return (BasicDatabase) Proxy.newProxyInstance(BasicDatabase.class.getClassLoader(), new Class<?>[] { BasicDatabase.class },
        (proxy, method, args) -> {
          try {
            return method.invoke(target, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private ShowCommandTail.Table apply(final String query) {
    return ShowCommandTail.apply(database, query, DATABASE_FIELDS, DATABASE_ROWS, Map.of());
  }

  private ShowCommandTail.Table call(final String query) {
    return ShowCommandTail.apply(database, query, COMPONENT_FIELDS, COMPONENT_ROWS, Map.of());
  }
}
