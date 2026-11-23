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
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The write targets of {@code SET}: the choices ArcadeDB makes where Neo4j differs (a null dynamic key writes nothing, a write to a
 * node deleted earlier in the statement is refused with a named error), the record a no-op label write leaves in place, and the
 * map forms {@code SET n += {...}} / {@code SET n = {...}} evaluated against the latest committed record, like the property form of
 * {@link CypherConcurrentIncrementTest}.
 */
class CypherSetWriteTargetsTest extends TestHelper {
  private List<Object> column(final String statement, final String column, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, params)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(column));
      }
    });
    return values;
  }

  private List<Long> numbers(final String statement, final String column, final Map<String, Object> params) {
    final List<Long> values = new ArrayList<>();
    for (final Object value : column(statement, column, params))
      values.add(((Number) value).longValue());
    return values;
  }

  private void run(final String statement) {
    database.transaction(() -> database.command("opencypher", statement).close());
  }

  @Test
  void aNullDynamicKeyWritesNothingAndTheOtherItemsOfTheClauseStillLand() {
    run("CREATE (:K {id: 1})");
    final Map<String, Object> params = new HashMap<>();
    params.put("k", null);

    assertThat(numbers("MATCH (n:K) SET n[$k] = 'y', n.z = 1 RETURN n.z AS z", "z", params)).containsExactly(1L);
    assertThat(numbers("MATCH (n:K) RETURN size(keys(n)) AS size", "size", Map.of())).containsExactly(2L);
  }

  @Test
  void aWriteToANodeDeletedEarlierInTheStatementNamesTheDeletedEntity() {
    run("CREATE (:T {id: 1})");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:T) DELETE n WITH n SET (CASE WHEN true THEN n END).v = 1").close()))
        .isInstanceOf(DeletedEntityException.class);
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:T) DELETE n WITH n SET n.v = 1").close())).isInstanceOf(
        DeletedEntityException.class);
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:T) DELETE n WITH n SET n:Z").close())).isInstanceOf(DeletedEntityException.class);
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:T) DELETE n WITH n SET n += {v: 1}").close())).isInstanceOf(
        DeletedEntityException.class);

    assertThat(numbers("MATCH (n:T) RETURN n.id AS id", "id", Map.of())).containsExactly(1L);
  }

  @Test
  void aLabelTheNodeAlreadyHasKeepsItsRecordAndItsRelationships() {
    run("CREATE (:T:U {id: 1})-[:R {w: 1}]->(:T {id: 2})");
    final List<Object> before = column("MATCH (n:T {id: 1}) RETURN id(n) AS i", "i", Map.of());

    run("MATCH (n:T {id: 1}) SET n:U");
    run("MATCH (n:T {id: 1}) SET n:T, n:U");

    assertThat(column("MATCH (n:T {id: 1}) RETURN id(n) AS i", "i", Map.of())).isEqualTo(before);
    assertThat(numbers("MATCH (:T {id: 1})-[r:R]->(:T {id: 2}) RETURN r.w AS w", "w", Map.of())).containsExactly(1L);
  }

  @Test
  void concurrentIncrementsThroughAMapMergeLoseNoUpdate() throws Exception {
    assertEveryCommittedIncrementIsCounted("MATCH (c:Cnt {id: 1}) SET c += {n: c.n + 1} RETURN c.n AS n");
  }

  @Test
  void concurrentIncrementsThroughAMapReplaceLoseNoUpdate() throws Exception {
    assertEveryCommittedIncrementIsCounted("MATCH (c:Cnt {id: 1}) SET c = {id: 1, n: c.n + 1} RETURN c.n AS n");
  }

  @Test
  void concurrentIncrementsThroughAPropertySetLoseNoUpdate() throws Exception {
    assertEveryCommittedIncrementIsCounted("MATCH (c:Cnt {id: 1}) SET c.n = c.n + 1 RETURN c.n AS n");
  }

  private void assertEveryCommittedIncrementIsCounted(final String increment) throws Exception {
    run("CREATE (:Cnt {id: 1, n: 0})");
    final int threads = 6;
    final int perThread = 40;
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<Integer>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < threads; t++)
        futures.add(executor.submit(() -> {
          start.await();
          int committed = 0;
          for (int i = 0; i < perThread; i++) {
            database.transaction(() -> database.command("opencypher", increment).close(), false, 200);
            committed++;
          }
          return committed;
        }));
      start.countDown();
      int total = 0;
      for (final Future<Integer> future : futures)
        total += future.get(120, TimeUnit.SECONDS);

      assertThat(numbers("MATCH (c:Cnt {id: 1}) RETURN c.n AS n", "n", Map.of())).containsExactly((long) total);
    } finally {
      executor.shutdownNow();
    }
  }
}
