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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A search that starts the graph build of a vector index, and whose build a DROP INDEX then cancels, answered with the bare
 * {@link CancellationException} of the cancelled build: HTTP 500, while a search arriving just after the drop answers 400 by
 * the missing index (issue #11849). It now gets the same 400 class, {@link CommandSQLParsingException}, naming the index.
 * <p>
 * A search builds on its own thread when the index is small, and hands the build to a thread of its own, rethrowing that
 * build's failure to the search, when it is not: both routes are pinned.
 */
class Issue11849SearchBuildCancelledByDropTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  /** Below {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}: the search builds on its own thread, taking no rebuild permit. */
  private static final int SMALL      = 50;
  /** Above it: the build is handed to a thread of its own, which takes a rebuild permit. */
  private static final int HANDED_OFF = 1_200;

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void searchWhoseBuildIsCancelledByADropAnswersByTheMissingIndex() throws Exception {
    assertSearchWhoseBuildIsCancelledByADropAnswersByTheMissingIndex(SMALL);
  }

  /**
   * The same drop, met by a build handed off to its own thread: the cancellation is the failure of that thread, rethrown to
   * the search that waited for it. Tagged {@code vector} because the build queues for the JVM-wide rebuild permit, which
   * convoys with the other vector tests that wait on a rebuild.
   */
  @Test
  @Tag("vector")
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void searchWhoseHandedOffBuildIsCancelledByADropAnswersByTheMissingIndex() throws Exception {
    assertSearchWhoseBuildIsCancelledByADropAnswersByTheMissingIndex(HANDED_OFF);
  }

  /**
   * The other half of the guard: a cancellation of the build on an index that is still valid is not a drop, and keeps its
   * type instead of being reported as a missing index.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void cancellationOfTheBuildOfAnIndexThatIsStillValidKeepsItsType() {
    final Random random = new Random(11849);
    final LSMVectorIndex index = createIndexAndLoad(random, SMALL);

    index.setAfterLiveSetReadForTest(() -> {
      throw new CancellationException("build cancelled, the index is not dropped");
    });
    try {
      final float[] query = vector(random);
      assertThatThrownBy(() -> index.findNeighborsFromVector(query, 5, 64)).isInstanceOf(CancellationException.class);
      assertThat(index.isValid()).isTrue();
    } finally {
      index.setAfterLiveSetReadForTest(null);
    }
  }

  private void assertSearchWhoseBuildIsCancelledByADropAnswersByTheMissingIndex(final int vectors) throws Exception {
    final Random random = new Random(11849);
    final LSMVectorIndex index = createIndexAndLoad(random, vectors);
    final String indexName = database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getName();

    // The build the search starts reaches this point after reading the live set. The index is dropped there, on another
    // thread, and the build is then cancelled the way a drop cancels a build in flight: the pool's shutdownNow() interrupts
    // the build workers, whose next vector read throws CancellationException (ArcadePageVectorValues.abortIfInterrupted).
    // Thrown here rather than raced against a real worker read, so the outcome does not depend on timing. On a build handed
    // off to its own thread the drop interrupts that very thread, which ends the wait for the dropper below at once
    final AtomicReference<Thread> dropper = new AtomicReference<>();
    final AtomicBoolean dropped = new AtomicBoolean();
    index.setAfterLiveSetReadForTest(() -> {
      final Thread thread = new Thread(() -> {
        database.getSchema().dropIndex(indexName);
        dropped.set(true);
      }, "issue-11849-dropper");
      dropper.set(thread);
      thread.start();
      try {
        thread.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new CancellationException("build cancelled by the drop of " + indexName);
    });

    final float[] query = vector(random);
    assertThatThrownBy(() -> search(query)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining(index.getName());

    assertThat(dropper.get()).as("the search's build ran").isNotNull();
    dropper.get().join(TimeUnit.MINUTES.toMillis(1));
    assertThat(dropped.get()).as("the drop ran while the search's build was in flight").isTrue();
    assertThat(index.isValid()).isFalse();
  }

  private LSMVectorIndex createIndexAndLoad(final Random random, final int vectors) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });
    database.transaction(() -> {
      for (int i = 0; i < vectors; i++)
        database.newDocument("Doc").set("vector", vector(random)).save();
    });
    return (LSMVectorIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private void search(final float[] query) {
    try (final ResultSet rs = database.query("sql", "SELECT expand(vector.neighbors('Doc[vector]', ?, 5))", (Object) query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
