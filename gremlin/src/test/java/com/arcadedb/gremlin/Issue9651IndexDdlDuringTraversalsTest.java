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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9651: {@code ArcadeFilterByIndexStep} held the index its traversal was compiled against and read it with no guard, so
 * a traversal overlapping a {@code CREATE INDEX} or {@code DROP INDEX} raised an {@code IndexException} or "File with id n was
 * not found", and sometimes answered a wrong count as a success. The step now checks the index is still usable each time it
 * opens a scan, takes the one now indexing the same property or scans the type when there is none, and the strategy no longer
 * fails the traversal when the index it is looking at goes away while it decides.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9651IndexDdlDuringTraversalsTest {
  private static final int READERS = 6;
  private static final int ROUNDS  = 60;
  private static final int ROWS    = 300;
  private static final int VALUES  = 10;

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-9651-index-ddl");
    final BasicDatabase database = graph.getDatabase();
    final DocumentType type = database.getSchema().createVertexType("T");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("T").set("a", i % VALUES).set("b", i).save();
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void aScanOpenedAfterItsIndexWasDroppedScansTheTypeInstead() {
    graph.getDatabase().getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    final var traversal = graph.traversal().V().hasLabel("T").has("a", 3).count();
    traversal.asAdmin().applyStrategies();
    assertThat(traversal.asAdmin().getSteps().stream().anyMatch(s -> s instanceof ArcadeFilterByIndexStep)).isTrue();

    graph.getDatabase().getSchema().dropIndex("T[a]");

    assertThat(traversal.next()).isEqualTo((long) ROWS / VALUES);
  }

  @Test
  void aScanOpenedAfterItsIndexWasRebuiltReadsTheNewOne() {
    graph.getDatabase().getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    final var traversal = graph.traversal().V().hasLabel("T").has("a", P.gte(8)).count();
    traversal.asAdmin().applyStrategies();

    graph.getDatabase().getSchema().dropIndex("T[a]");
    graph.getDatabase().getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");

    assertThat(traversal.next()).isEqualTo((long) ROWS / VALUES * 2);
  }

  @Test
  void aScanWhoseIndexIsDroppedMidwayFinishesOnTheTypeWithoutRepeatingACandidate() {
    graph.getDatabase().getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    final var traversal = graph.traversal().V().hasLabel("T").has("a", P.gte(0)).id();
    final Set<Object> ids = new HashSet<>();
    for (int i = 0; i < 10; i++)
      assertThat(ids.add(traversal.next())).isTrue();

    // deterministic, unlike the churn below: the drop lands between two candidates of the same scan
    graph.getDatabase().getSchema().dropIndex("T[a]");

    while (traversal.hasNext())
      assertThat(ids.add(traversal.next())).as("a candidate emitted twice").isTrue();
    assertThat(ids).hasSize(ROWS);
  }

  /**
   * Past {@link ArcadeFilterByIndexStep#MAX_TRACKED_CANDIDATES} the scan stops remembering what it emitted, so an index lost
   * after that point fails the traversal with a retryable error rather than answering short or repeating candidates.
   */
  @Test
  @Tag("slow")
  void anIndexLostAfterTooManyCandidatesFailsTheTraversalInsteadOfAnsweringShort() {
    final BasicDatabase database = graph.getDatabase();
    database.getSchema().createVertexType("Many").createProperty("a", Type.INTEGER);
    final int total = ArcadeFilterByIndexStep.MAX_TRACKED_CANDIDATES + 100;
    database.transaction(() -> {
      for (int i = 0; i < total; i++)
        database.newVertex("Many").set("a", i).save();
    });
    database.getSchema().getType("Many").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");

    final var traversal = graph.traversal().V().hasLabel("Many").has("a", P.gte(0)).id();
    for (int i = 0; i < ArcadeFilterByIndexStep.MAX_TRACKED_CANDIDATES + 10; i++)
      traversal.next();

    database.getSchema().dropIndex("Many[a]");

    assertThatThrownBy(() -> {
      while (traversal.hasNext())
        traversal.next();
    }).isInstanceOf(IndexException.class).hasMessageContaining("run the traversal again");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void traversalsKeepAnsweringWhileAnUnrelatedIndexIsCreatedAndDropped() throws Exception {
    graph.getDatabase().getSchema().getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    churn("b");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void traversalsKeepAnsweringWhileTheirOwnIndexIsCreatedAndDropped() throws Exception {
    churn("a");
  }

  /**
   * Readers count by an equality and by a range on {@code a} while a writer creates and drops the index on {@code property}.
   * Every count must be the quiescent one: an answer is either right or an error, and here none may be an error.
   */
  private void churn(final String property) throws Exception {
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final AtomicLong wrongCounts = new AtomicLong();
    final Map<String, AtomicLong> failures = new ConcurrentHashMap<>();
    final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    final AtomicReference<String> firstWrong = new AtomicReference<>();
    final List<Thread> readers = new ArrayList<>();
    for (int t = 0; t < READERS; t++) {
      final Random random = new Random(t);
      final boolean range = t % 2 == 1;
      final Thread thread = new Thread(() -> {
        while (!stop.get()) {
          final int value = random.nextInt(VALUES);
          try {
            final var traversal = range ?
                graph.traversal().V().hasLabel("T").has("a", P.gte(value)).count() :
                graph.traversal().V().hasLabel("T").has("a", value).count();
            final long count = traversal.next();
            final long expected = range ? (long) (VALUES - value) * (ROWS / VALUES) : ROWS / VALUES;
            if (count != expected) {
              wrongCounts.incrementAndGet();
              firstWrong.compareAndSet(null, (range ? "a >= " : "a = ") + value + ": " + count + " instead of " + expected + " by "
                  + traversal);
            }
            answered.incrementAndGet();
          } catch (final Throwable e) {
            firstFailure.compareAndSet(null, e);
            failures.computeIfAbsent(e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0],
                k -> new AtomicLong()).incrementAndGet();
          }
        }
      });
      readers.add(thread);
      thread.start();
    }

    try {
      final Schema schema = graph.getDatabase().getSchema();
      for (int i = 0; i < ROUNDS; i++) {
        schema.getType("T").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, property);
        schema.dropIndex("T[" + property + "]");
      }
    } finally {
      stop.set(true);
      for (final Thread thread : readers)
        thread.join();
    }

    assertThat(answered.get()).isGreaterThan(0);
    assertThat(failures).as("traversals answered: " + answered.get() + ", first failure: " + stackTrace(firstFailure.get())).isEmpty();
    assertThat(wrongCounts.get()).as("wrong counts answered as a success, of " + answered.get() + ", first: " + firstWrong.get())
        .isZero();
  }

  private static String stackTrace(final Throwable e) {
    if (e == null)
      return "none";
    final StringWriter out = new StringWriter();
    e.printStackTrace(new PrintWriter(out));
    return out.toString();
  }
}
