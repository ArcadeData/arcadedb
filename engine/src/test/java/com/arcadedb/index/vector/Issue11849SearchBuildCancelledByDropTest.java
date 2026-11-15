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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A search that starts the graph build of a vector index, and whose build a DROP INDEX then cancels, answered with the bare
 * {@link CancellationException} of the cancelled build: HTTP 500, while a search arriving just after the drop answers 400 by
 * the missing index (issue #11849). It now gets the same 400 class, {@link CommandSQLParsingException}, naming the index.
 */
class Issue11849SearchBuildCancelledByDropTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int VECTORS    = 50;

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void searchWhoseBuildIsCancelledByADropAnswersByTheMissingIndex() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });
    final Random random = new Random(11849);
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++)
        database.newDocument("Doc").set("vector", vector(random)).save();
    });
    final LSMVectorIndex index = (LSMVectorIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
    final String indexName = database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getName();

    // The build the search starts reaches this point after reading the live set. The index is dropped there, on another
    // thread, and the build is then cancelled the way a drop cancels a build in flight: the pool's shutdownNow() interrupts
    // the build workers, whose next vector read throws CancellationException (ArcadePageVectorValues.abortIfInterrupted).
    // Thrown here rather than raced against a real worker read, so the outcome does not depend on timing
    final AtomicBoolean dropped = new AtomicBoolean();
    index.setAfterLiveSetReadForTest(() -> {
      final Thread dropper = new Thread(() -> {
        database.getSchema().dropIndex(indexName);
        dropped.set(true);
      }, "issue-11849-dropper");
      dropper.start();
      try {
        dropper.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new CancellationException("build cancelled by the drop of " + indexName);
    });

    final float[] query = vector(random);
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT expand(vector.neighbors('Doc[vector]', ?, 5))", (Object) query)) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining(index.getName());
    assertThat(dropped.get()).as("the drop ran while the search's build was in flight").isTrue();
    assertThat(index.isValid()).isFalse();
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
