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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6536 (follow-up from #6527/#6528): making {@code LSMVectorIndex.graphFile} {@code volatile} fixed the
 * cross-thread <em>visibility</em> gap #6527 was about, but every read site still does a {@code graphFile != null}
 * check followed by one or more <em>separate</em> reads of the same field to dereference it, e.g.
 * {@link Index#getFileIds()}:
 * <pre>
 *   if (graphFile != null)
 *     return List.of(mutable.getFileId(), graphFile.getFileId());
 * </pre>
 * During {@code COMPACT INDEX}, the graph-builder thread briefly sets {@code graphFile = null} before registering
 * the fresh post-compaction file, guarded by {@code graphBuildLock} on the writer side only - a reader taking no
 * lock at all can have its null-check pass just before that write and its dereference land just after, throwing an
 * NPE instead of returning a torn-but-non-null answer. Making the field {@code volatile} in #6528 made this window
 * reliably <em>visible</em> across threads for the first time, so the TOCTOU race is more likely to manifest as a
 * live NPE now than it was pre-#6528 (a plain field's CPU-local caching used to incidentally hide the window).
 * <p>
 * This test reproduces the race deterministically rather than relying on real {@code COMPACT INDEX} timing (that
 * null window is only a handful of statements wide - far too narrow to hit reliably by chance): a background thread
 * hammers {@link Index#getFileIds()} while the field is toggled null/non-null many times via reflection, exactly
 * mirroring the two states {@code buildGraphFromScratchExclusively()} puts it through around the persist step. Pinned
 * against pre-fix code (a bare double read of {@code graphFile}), this fails with an NPE almost immediately; against
 * the fix (a single local snapshot taken once per read site), it must never fail.
 */
@Tag("vector")
class LSMVectorIndexGraphFileToctouTest {
  private static final int    DIMENSIONS        = 16;
  private static final int    NUM_VECTORS       = 50;
  private static final String DB_PATH           = "./target/databases/LSMVectorIndexGraphFileToctouTest";
  private static final int    TOGGLE_ITERATIONS = 50_000;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void getFileIdsMustNotNpeWhileGraphFileIsBrieflyNulledOnAnotherThread() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.transaction(() -> {
        final DocumentType t = db.getSchema().createDocumentType("Doc");
        t.createProperty("id", Type.INTEGER);
        t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
      });
      db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
          + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"maxConnections\": 16 }");
      waitForGraph(db);

      final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];

      final Field field = LSMVectorIndex.class.getDeclaredField("graphFile");
      field.setAccessible(true);
      final Object originalGraphFile = field.get(lsm);
      assertThat(originalGraphFile)
          .as("index must already have registered a graph file for the null/non-null toggle below to be meaningful")
          .isNotNull();

      final AtomicBoolean         stop          = new AtomicBoolean(false);
      final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
      final Thread reader = new Thread(() -> {
        while (!stop.get()) {
          try {
            lsm.getFileIds();
          } catch (final Throwable t) {
            readerFailure.compareAndSet(null, t);
            return;
          }
        }
      }, "graphFile-toctou-reader");
      reader.start();

      try {
        for (int i = 0; i < TOGGLE_ITERATIONS && readerFailure.get() == null; i++) {
          field.set(lsm, null);
          field.set(lsm, originalGraphFile);
        }
      } finally {
        stop.set(true);
        reader.join(10_000);
      }

      assertThat(readerFailure.get())
          .as("getFileIds() must take a single local snapshot of the volatile graphFile field before checking and "
              + "dereferencing it; reading it twice lets a concurrent writer null it out in between and turns the "
              + "null-check-then-use into a live NullPointerException (issue #6536)")
          .isNull();

      db.close();
    }
  }

  private static void waitForGraph(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    for (int i = 0; i < 600 && lsm.getStats().get("graphNodeCount") == 0L; i++)
      try { Thread.sleep(50); } catch (final InterruptedException ignored) { break; }
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
