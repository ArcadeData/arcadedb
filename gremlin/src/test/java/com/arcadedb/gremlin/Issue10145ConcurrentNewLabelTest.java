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

import com.arcadedb.exception.NeedRetryException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10145: a type a write needs implicitly is created "if not exists", so two writers introducing the same new label at the
 * same time both end with it. {@code ArcadeGraph.addVertex} and {@code ArcadeVertex.addEdge} checked {@code existsType()} and then
 * called {@code createVertexType()} / {@code createEdgeType()}, so the thread losing the race failed with a non-retryable
 * {@code TypeAlreadyExistsException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10145ConcurrentNewLabelTest {
  private static final int THREADS = 4;
  private static final int ROUNDS  = 5;
  private static final int RETRIES = 50;

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-10145-concurrent-new-label");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void concurrentAddVertexOfANewLabelNeverFailsForGood() throws Exception {
    for (int round = 0; round < ROUNDS; round++) {
      final String label = "NewVertexLabel" + round;
      final List<Throwable> failures = runConcurrently(() -> graph.addVertex(T.label, label));
      assertThat(failures).as("round %d", round).isEmpty();
      assertThat(graph.getDatabase().countType(label, false)).isEqualTo(THREADS);
    }
  }

  @Test
  void concurrentAddEdgeOfANewLabelNeverFailsForGood() throws Exception {
    for (int round = 0; round < ROUNDS; round++) {
      final String label = "NewEdgeLabel" + round;
      final Object[] ends = new Object[2];
      graph.getDatabase().transaction(() -> {
        ends[0] = graph.addVertex(T.label, "End").id();
        ends[1] = graph.addVertex(T.label, "End").id();
      });

      final List<Throwable> failures = runConcurrently(() -> {
        final Vertex from = graph.vertices(ends[0]).next();
        final Vertex to = graph.vertices(ends[1]).next();
        from.addEdge(label, to);
      });
      assertThat(failures).as("round %d", round).isEmpty();
      assertThat(graph.getDatabase().countType(label, false)).isEqualTo(THREADS);
    }
  }

  private List<Throwable> runConcurrently(final Runnable write) throws Exception {
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final CyclicBarrier barrier = new CyclicBarrier(THREADS);
    final Thread[] threads = new Thread[THREADS];
    for (int t = 0; t < THREADS; t++) {
      threads[t] = new Thread(() -> {
        try {
          barrier.await(30, TimeUnit.SECONDS);
          for (int attempt = 0; ; attempt++) {
            try {
              graph.getDatabase().transaction(write::run);
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
      // A thread still running recorded nothing, and would read as a write that succeeded
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
}
