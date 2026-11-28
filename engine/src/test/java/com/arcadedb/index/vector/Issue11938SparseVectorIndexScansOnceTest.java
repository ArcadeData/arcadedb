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
import com.arcadedb.database.RID;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An LSM_VECTOR index on a property only some records carry (issue #11938). Its pages always yield fewer vectors than its
 * bucket has records, so the issue #3722 cross-check scanned the whole bucket and logged a WARNING on every graph build,
 * to recover nothing. A scan that recovered nothing is now not repeated until the index is loaded again, and only a scan
 * that recovered something is a WARNING.
 */
class Issue11938SparseVectorIndexScansOnceTest extends TestHelper {
  private static final int    DIMENSIONS     = 8;
  private static final int    VECTORS        = 20;
  private static final int    WITHOUT_VECTOR = 100;
  private static final String SCAN_MESSAGE   = "Falling back to document scan";

  private final Random random = new Random(11938);

  private record LogLine(Level level, String message) {
  }

  @Test
  void aSparseIndexScansItsBucketOncePerLoadAndOnlyARecoveryWarns() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql",
          "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'EUCLIDEAN'}");
      for (int i = 0; i < VECTORS; i++)
        database.newDocument("Doc").set("vector", vector()).save();
      for (int i = 0; i < WITHOUT_VECTOR; i++)
        database.newDocument("Doc").set("name", "no vector " + i).save();
    });

    final float[] added = vector();
    final RID[] addedRid = new RID[1];
    final List<LogLine> builds = capture(() -> {
      index().buildVectorGraphNow();
      // A write and a second build: the pages still yield far fewer vectors than the bucket has records
      database.transaction(() -> addedRid[0] = database.newDocument("Doc").set("vector", added).save().getIdentity());
      index().buildVectorGraphNow();
    });

    assertThat(scans(builds)).as("one document scan per load of the index, not one per build (issue #11938)").isEqualTo(1);
    assertThat(builds.stream().filter(l -> l.level() == Level.WARNING).map(LogLine::message))
        .as("a scan that recovered nothing is no WARNING").isEmpty();
    database.transaction(() -> assertThat(index().findNeighborsFromVector(added, 1).stream().map(Pair::getFirst))
        .as("the second build, without the scan, still indexed the new vector").containsExactly(addedRid[0]));

    // A fresh load of the index cross-checks its pages again
    reopenDatabase();
    final List<LogLine> afterReopen = capture(() -> index().buildVectorGraphNow());
    assertThat(scans(afterReopen)).as("the cross-check still reaches the document scan once the index is loaded again")
        .isEqualTo(1);
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
  }

  private static long scans(final List<LogLine> log) {
    return log.stream().filter(l -> l.message().contains(SCAN_MESSAGE)).count();
  }

  /** The lines the vector index logged while {@code action} ran, at every level. */
  private static List<LogLine> capture(final Runnable action) {
    final List<LogLine> lines = new CopyOnWriteArrayList<>();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        if (requester instanceof LSMVectorIndex && message != null)
          lines.add(new LogLine(level, message));
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (requester instanceof LSMVectorIndex && message != null)
          lines.add(new LogLine(level, message));
      }

      @Override
      public void flush() {
      }
    });
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }
    return lines;
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
