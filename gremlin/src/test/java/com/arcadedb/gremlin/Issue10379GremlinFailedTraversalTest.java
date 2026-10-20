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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10379: a Gremlin traversal that fails while its result is read, after it wrote part of its records inside the caller's
 * transaction, left that part behind and the caller's commit published it. The traversal runs when the result set is read, after
 * {@code command()} has returned, so the check of the dispatcher cannot see it: the result set carries the same check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10379GremlinFailedTraversalTest {
  private static final String PATH = "./target/test-gremlin-10379-failed-traversal";

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    database = (Database) graph.getDatabase();
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.n INTEGER");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    database.command("sql", "CREATE INDEX ON T (v) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex("T").set("n", i).set("v", 100 + i).save();
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void aTraversalThatFailsWhileItsResultIsReadCannotBeCommittedHalfApplied() {
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('T').property('v', 1)")) {
        while (rs.hasNext())
          rs.next();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0)).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(caught.get()).as("the traversal failed").isNotNull();
    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void aTraversalThatOpensTheTransactionItselfAndFailsCannotBeCommittedHalfApplied() {
    // no transaction is open when the traversal starts: the graph opens one while the result is read, and it is the traversal's
    assertThat(database.isTransactionActive()).isFalse();
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('T').property('v', 1)")) {
      while (rs.hasNext())
        rs.next();
    } catch (final RuntimeException e) {
      caught.set(e);
    }

    assertThat(caught.get()).as("the traversal failed").isNotNull();
    assertThat(database.isTransactionActive()).as("the traversal left its transaction open").isTrue();
    assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");
    database.rollback();
    assertThat(values()).containsExactly(100, 101, 102);
  }

  @Test
  void aTraversalThatFailsWithoutWritingLeavesAReusedTransactionContextUsable() {
    // the context of an earlier transaction keeps counting: the writes it already made are not the traversal's
    database.transaction(() -> database.command("sql", "UPDATE T SET v = 500 WHERE n = 2").close());

    try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('T').has('n', 0).property('v', 'abc')")) {
      while (rs.hasNext())
        rs.next();
    } catch (final RuntimeException e) {
      assertThat(e).isNotNull();
    }
    if (database.isTransactionActive())
      database.commit();

    assertThat(values()).containsExactly(100, 101, 500);
  }

  /**
   * A traversal that ends in a terminal step runs inside the call, so the failure is raised where the dispatcher catches it. A query
   * of Gremlin can write, which one of SQL and openCypher cannot: this covers the check of {@code query()} and of {@code command()}.
   */
  @ParameterizedTest
  @ValueSource(strings = { "command", "query" })
  void aTraversalThatFailsInsideTheCallCannotBeCommittedHalfApplied(final String call) {
    final String traversal = "g.V().hasLabel('T').property('v', 1).toList()";
    final AtomicReference<Throwable> caught = new AtomicReference<>();

    assertThatThrownBy(() -> database.transaction(() -> {
      try {
        (call.equals("command") ? database.command("gremlin", traversal) : database.query("gremlin", traversal)).close();
      } catch (final RuntimeException e) {
        caught.set(e);
      }
    }, false, 0)).as(call).isInstanceOf(TransactionException.class).hasMessageContaining("cannot be committed");

    assertThat(caught.get()).as(call + " failed").isNotNull();
    assertThat(values()).as(call).containsExactly(100, 101, 102);
  }

  @Test
  void aTraversalThatSucceedsCommitsNormally() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('T').has('n', 2).property('v', 500)")) {
        while (rs.hasNext())
          rs.next();
      }
    }, false, 0);

    assertThat(values()).containsExactly(100, 101, 500);
  }

  private List<Integer> values() {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T ORDER BY n")) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("v").intValue());
    }
    return out;
  }
}
