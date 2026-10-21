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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10145: a type an openCypher write needs implicitly is created "if not exists", so another writer creating the same
 * one at the same time is never an error for either of them. The composite type of a multi-label node ({@code A~B}) broke
 * that: {@code Labels.ensureCompositeType} checked {@code existsType()} and then ran a builder WITHOUT
 * {@code withIgnoreIfExists(true)}, so the thread losing the race got a {@code TypeAlreadyExistsException} - a
 * {@code SchemaException}, not a {@code NeedRetryException} - on the first concurrent round, every time. The
 * {@code merge.relationship} procedure had the same check-then-act for its edge type.
 * <p>
 * Every shape below introduces brand-new types from several threads released by one barrier, and the rule asserted is the one
 * the implicit-type path promises: a refusal along the way may only be a retryable one, and every statement ends with its
 * record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10145ConcurrentImplicitTypeCreationTest extends TestHelper {
  private static final int THREADS = 4;
  private static final int ROUNDS  = 5;
  private static final int RETRIES = 50;

  @ParameterizedTest
  @ValueSource(strings = { "singleLabel", "create", "merge", "setLabels", "setDynamicLabels", "mergeNode", "relationship",
      "mergeRelationship" })
  void concurrentStatementsIntroducingTheSameTypesNeverFailForGood(final String shape) throws Exception {
    for (int round = 0; round < ROUNDS; round++) {
      final String a = "A" + round;
      final String b = "B" + round;
      final String base = "Base" + round;
      final String rel = "R" + round;

      if (shape.startsWith("set"))
        // The node to relabel exists beforehand: only the labels the SET adds are new
        database.transaction(() -> {
          for (int t = 0; t < THREADS; t++)
            database.command("opencypher", "CREATE (:" + base + " {t: $t})", Map.of("t", t)).close();
        });

      final String statement = switch (shape) {
        case "singleLabel" -> "CREATE (n:" + a + " {t: $t})";
        case "create" -> "CREATE (n:" + a + ":" + b + " {t: $t})";
        case "merge" -> "MERGE (n:" + a + ":" + b + " {t: $t})";
        case "setLabels" -> "MATCH (n:" + base + " {t: $t}) SET n:" + a + ":" + b;
        case "setDynamicLabels" -> "MATCH (n:" + base + " {t: $t}) SET n:$(['" + a + "', '" + b + "'])";
        case "mergeNode" -> "CALL merge.node(['" + a + "', '" + b + "'], {t: $t}, {}, {}) YIELD node RETURN node";
        case "relationship" -> "CREATE (:" + a + ":" + b + " {t: $t})-[:" + rel + "]->(:" + a + ")";
        case "mergeRelationship" -> "CREATE (x:" + a + " {t: $t}), (y:" + a + ") WITH x, y CALL merge.relationship(x, '" + rel
            + "', {t: $t}, {}, y, {}) YIELD rel RETURN rel";
        default -> throw new IllegalArgumentException(shape);
      };

      final List<Throwable> failures = runConcurrently(statement);
      assertThat(failures).as("round %d of '%s': a refusal that is not retryable", round, shape).isEmpty();

      final String expectedType = switch (shape) {
        case "singleLabel", "mergeRelationship" -> a;
        case "setLabels", "setDynamicLabels" -> a + "~" + b + "~" + base;
        default -> a + "~" + b;
      };
      assertThat(database.getSchema().existsType(expectedType)).isTrue();
      final DocumentType type = database.getSchema().getType(expectedType);
      assertThat(type.instanceOf(a)).isTrue();
      if (!expectedType.equals(a))
        assertThat(type.instanceOf(b)).isTrue();
      assertThat(count("MATCH (n:" + a + ") WHERE n.t IS NOT NULL RETURN count(n) AS c")).isEqualTo(THREADS);
      if (shape.endsWith("elationship"))
        assertThat(count("MATCH ()-[r:" + rel + "]->() RETURN count(r) AS c")).isEqualTo(THREADS);
    }
  }

  private List<Throwable> runConcurrently(final String statement) throws Exception {
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final CyclicBarrier barrier = new CyclicBarrier(THREADS);
    final Thread[] threads = new Thread[THREADS];
    for (int t = 0; t < THREADS; t++) {
      final int threadId = t;
      threads[t] = new Thread(() -> {
        try {
          barrier.await(30, TimeUnit.SECONDS);
          for (int attempt = 0; ; attempt++) {
            try {
              database.transaction(() -> database.command("opencypher", statement, Map.of("t", threadId)).close());
              return;
            } catch (final Throwable e) {
              if (!isRetryable(e) || attempt >= RETRIES) {
                failures.add(e);
                return;
              }
            }
          }
        } catch (final Throwable e) {
          failures.add(e);
        }
      });
      threads[t].start();
    }
    for (final Thread thread : threads) {
      thread.join(TimeUnit.MINUTES.toMillis(2));
      // A thread still running recorded nothing, and would read as a statement that succeeded
      assertThat(thread.isAlive()).as("a writer still running after its join: hung").isFalse();
    }
    return failures;
  }

  private static boolean isRetryable(final Throwable e) {
    for (Throwable c = e; c != null; c = c.getCause())
      if (c instanceof NeedRetryException)
        return true;
    return false;
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
