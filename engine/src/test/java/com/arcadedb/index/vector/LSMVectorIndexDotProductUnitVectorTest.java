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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5750: an {@code LSM_VECTOR} index declared with {@code DOT_PRODUCT} used to accept
 * vectors of any magnitude in silence.
 * <p>
 * {@code DOT_PRODUCT} is the cheap path for cosine on data that is already normalized, and JVector documents unit
 * length as its precondition. Broken, the raw dot product is unbounded below, so a real vector can score under the
 * zero an unreadable node is scored at and waste beam budget, and rankings come out quietly worse than the
 * {@code COSINE} the user did not pick. The build now says so.
 * <p>
 * The three cases pinned here are what makes the warning useful rather than noise: it fires when the precondition is
 * broken, it stays silent when it is honoured, and it does not repeat. The last one is not cosmetic - a bulk ingest
 * crosses the rebuild threshold every time the pending set grows by a fraction of the graph, so an unguarded warning
 * would be emitted a hundred-odd times over a million-row load.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexDotProductUnitVectorTest extends TestHelper {

  private static final int DIMENSIONS  = 16;
  private static final int NUM_VECTORS = 40;

  @Test
  void aDotProductIndexWarnsOnceAboutNonUnitVectors() {
    // Capture from before the first vector exists: any build at all has to be inside the window, or a rebuild that
    // beat the assertions to it would consume the one warning and the test would read the silence as a regression.
    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger originalLogger = swapInCapturingLogger(warnings);
    try {
      createIndexAndPopulate("DOT_PRODUCT", false);
      final LSMVectorIndex index = bucketIndex();

      index.buildVectorGraphNow();
      assertThat(unitLengthWarnings(warnings))
          .as("a DOT_PRODUCT build over vectors that are not unit length must warn (captured=%s)", warnings)
          .hasSize(1);
      assertThat(unitLengthWarnings(warnings).getFirst())
          .as("the warning must name the index and say what to do about it")
          .contains("DOT_PRODUCT").contains("COSINE").contains(index.getName());

      // Every later rebuild re-reads the same non-unit vectors. Without the once-per-index guard each one repeats
      // the warning, which under sustained ingestion is the actual operational cost of this fix.
      index.buildVectorGraphNow();
      index.buildVectorGraphNow();
      assertThat(unitLengthWarnings(warnings))
          .as("the warning is reported once per index, not once per rebuild (captured=%s)", warnings)
          .hasSize(1);
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  @Test
  void aDotProductIndexOverUnitVectorsStaysSilent() {
    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger originalLogger = swapInCapturingLogger(warnings);
    try {
      createIndexAndPopulate("DOT_PRODUCT", true);
      bucketIndex().buildVectorGraphNow();
      assertThat(unitLengthWarnings(warnings))
          .as("normalized vectors honour the precondition, so there is nothing to report (captured=%s)", warnings)
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  /**
   * {@code COSINE} divides the magnitude out, so a non-unit vector is not a misconfiguration there. Warning on it
   * would train operators to ignore the message on the metric where it does mean something.
   */
  @Test
  void aCosineIndexOverNonUnitVectorsStaysSilent() {
    final List<String> warnings = new CopyOnWriteArrayList<>();
    final Logger originalLogger = swapInCapturingLogger(warnings);
    try {
      createIndexAndPopulate("COSINE", false);
      bucketIndex().buildVectorGraphNow();
      assertThat(unitLengthWarnings(warnings))
          .as("the unit-length precondition belongs to DOT_PRODUCT alone (captured=%s)", warnings)
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  private void createIndexAndPopulate(final String similarity, final boolean normalized) {
    // Per-database, so it cannot leak into the rest of the suite. Without it the inactivity timer can start a
    // rebuild on its own schedule, which is a second writer racing the explicit builds below.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": %d, "similarity": "%s"}\
          """.formatted(DIMENSIONS, similarity));
    });

    database.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++)
        database.newVertex("Doc").set("name", "doc" + i).set("embedding", embeddingOf(i, normalized)).save();
    });
  }

  private LSMVectorIndex bucketIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static List<String> unitLengthWarnings(final List<String> warnings) {
    return warnings.stream().filter(w -> w.contains("not unit length")).toList();
  }

  private static Logger swapInCapturingLogger(final List<String> warnings) {
    final Logger original = readLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings, original));
    return original;
  }

  private static Logger readLogger() {
    try {
      final Field field = LogManager.instance().getClass().getDeclaredField("logger");
      field.setAccessible(true);
      return (Logger) field.get(LogManager.instance());
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot read the active logger", e);
    }
  }

  /**
   * A pseudo-random direction per vertex from a fixed seed, scaled to a magnitude that is unmistakably not 1 unless
   * the caller asks for the normalized variant. The directions are random rather than structured so the fixture is
   * in general position: no two vertices are collinear and none sits on an axis, which is what keeps the graph build
   * from having to break ties.
   */
  private static float[] embeddingOf(final int i, final boolean normalized) {
    final Random random = new Random(i * 31L + 7);
    final float[] embedding = new float[DIMENSIONS];
    double magnitudeSquared = 0;
    for (int d = 0; d < DIMENSIONS; d++) {
      embedding[d] = random.nextFloat() * 2 - 1;
      magnitudeSquared += (double) embedding[d] * embedding[d];
    }

    // A random 16-dimensional direction already has a magnitude around 2.3, far outside the 1% tolerance, but scale
    // it anyway so the fixture states its own intent instead of relying on the dimension count.
    final double magnitude = Math.sqrt(magnitudeSquared);
    final double scale = normalized ? 1 / magnitude : (3.0 + i * 0.1) / magnitude;
    for (int d = 0; d < DIMENSIONS; d++)
      embedding[d] = (float) (embedding[d] * scale);

    return embedding;
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/LSMVectorIndexDotProductUnitVectorTest";
  }

  /**
   * Captures WARNING-and-above messages while forwarding every record to the production logger, so swapping it in
   * does not hide anything else the run logs. Swapping the LogManager logger bypasses JUL, so the assertions do not
   * depend on handler or level state another test in the same JVM may have left behind.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;
    private final Logger       delegate;

    CapturingLogger(final List<String> warnings, final Logger delegate) {
      this.warnings = warnings;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template, which still carries the text the assertions match on.
        }
      }
      warnings.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
