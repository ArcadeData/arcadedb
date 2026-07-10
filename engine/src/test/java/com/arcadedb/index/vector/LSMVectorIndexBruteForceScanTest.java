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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4581: the brute-force fallback in {@link LSMVectorIndex} must pair each
 * candidate RID with the score computed from that RID's own vector. Previously the scan re-read the
 * volatile {@code ordinalToVectorId} field while the {@code vectors} snapshot was captured from a
 * (potentially different) earlier reference, so an ordinal could be mapped through one array but
 * scored through another, returning RIDs paired with the wrong vector's score.
 *
 * The fix threads a single ordinal->vectorId snapshot through {@code findNeighborsFromVector} so the
 * vectors snapshot and the brute-force scan always use the same mapping. These tests force the
 * brute-force fallback path (a stale graph after a large delete) and assert the RID/score pairing is
 * correct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexBruteForceScanTest extends TestHelper {

  private static final int DIMENSIONS = 16;

  @Test
  void bruteForceFallbackPairsRidWithItsOwnScore() throws Exception {
    // Keep the graph stale after deletes: disable mutation-triggered and inactivity rebuilds so the
    // HNSW graph still references the deleted ordinals and graph search yields too few live results,
    // forcing the brute-force fallback over the surviving vectors.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // The graph must be "large" (>= 1000 nodes) so that the stale-graph delete does NOT trigger a
    // synchronous rebuild at search time (small graphs always rebuild synchronously, which would
    // compact the deleted ordinals away and hide the fallback). With a large graph and a delete
    // count below the (very high) rebuild threshold, no rebuild runs and the search keeps the stale
    // full graph - exactly the degraded-graph scenario the brute-force fallback exists for.
    final int total = 1200;
    final int kept = 50; // indices [total-kept, total) survive the delete

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("BFVec");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      // EUCLIDEAN: the stale graph still references deleted ordinals whose vectors are scored during
      // graph traversal; COSINE would reject those near-zero "deleted sentinel" vectors as undefined,
      // which is an unrelated guard. EUCLIDEAN keeps the focus on the brute-force RID/score pairing.
      database.getSchema().buildTypeIndex("BFVec", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("EUCLIDEAN")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final RID[] ridByIndex = new RID[total];
    final float[][] vectorByIndex = new float[total][];
    database.transaction(() -> {
      for (int i = 0; i < total; i++) {
        final float[] vec = createDeterministicVector(i, DIMENSIONS);
        final MutableVertex v = database.newVertex("BFVec").set("name", "v" + i).set("vector", vec);
        v.save();
        ridByIndex[i] = v.getIdentity();
        vectorByIndex[i] = vec;
      }
    });

    // Build the graph over all vectors.
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("BFVec[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();

    // Delete the first (total - kept) vertices WITHOUT rebuilding the graph.
    database.transaction(() -> {
      for (int i = 0; i < total - kept; i++)
        database.command("sql", "DELETE FROM BFVec WHERE name = ?", "v" + i);
    });

    final Set<RID> keptRids = new HashSet<>();
    for (int i = total - kept; i < total; i++)
      keptRids.add(ridByIndex[i]);

    // Capture WARNING logs to confirm the brute-force fallback actually fires.
    final List<String> captured = new CopyOnWriteArrayList<>();
    final Logger originalLogger = readField(LogManager.instance(), "logger");
    LogManager.instance().setLogger(new CapturingLogger(captured, originalLogger));
    try {
      // Query each surviving vertex with its own exact vector: the correct nearest neighbor is that
      // vertex itself with distance ~0. If a RID were paired with another vector's score, the exact
      // match would neither rank first nor have a near-zero distance.
      for (int probe = total - kept; probe < total; probe++) {
        final RID expected = ridByIndex[probe];
        // Request k = total: the stale graph still exposes all `total` ordinals (the deleted ones were
        // not rebuilt away), so `expectedResults` is `total` while graph search can only surface the
        // `kept` survivors. That gap (results < 80% of available) is what triggers the brute-force scan.
        final List<Result> results = executeVectorSearch(vectorByIndex[probe], total);

        assertThat(results)
            .as("Brute-force fallback must return the surviving vectors for probe v%d", probe)
            .isNotEmpty();

        // No deleted vertex may appear, and every returned RID must be one of the survivors.
        for (final Result r : results) {
          final RID rid = r.<Document>getProperty("record").getIdentity();
          assertThat(keptRids)
              .as("Returned RID %s must be a surviving (non-deleted) vertex", rid)
              .contains(rid);
        }

        final Result top = results.getFirst();
        final RID topRid = top.<Document>getProperty("record").getIdentity();
        final float topDistance = ((Number) top.getProperty("distance")).floatValue();

        assertThat(topRid)
            .as("Self-query for v%d must rank that exact vertex first (RID<->score pairing)", probe)
            .isEqualTo(expected);
        assertThat(topDistance)
            .as("Self-query distance for v%d must be ~0 (score computed from the same vector)", probe)
            .isLessThan(1e-3f);
      }

      assertThat(captured)
          .as("The brute-force fallback WARNING must fire, proving the scan path is exercised")
          .anyMatch(m -> m != null && m.contains("falling back to brute-force scan"));
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  /**
   * Deterministic reproduction of issue #4581. The {@code vectors} snapshot used for scoring is built
   * from one ordinal->vectorId array; we then reassign the volatile {@code ordinalToVectorId} field to
   * a different permutation (simulating a concurrent graph rebuild) before running the brute-force scan.
   * <p>
   * The scan must pair each ordinal's RID and score through the SAME array that {@code vectors} was built
   * from. If it instead re-read the live field, each ordinal would map to the wrong vectorId/RID while the
   * score still came from {@code vectors}, so the perfect self-match score would be attached to the wrong
   * RID. This test fails with the pre-fix code (live-field read) and passes once a single snapshot is
   * threaded through {@code bruteForceScan}.
   */
  @Test
  void bruteForceScanUsesTheSnapshotMatchingTheVectorsValues() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    final int total = 30;

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("BFInj");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("BFInj", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("EUCLIDEAN")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < total; i++)
        database.newVertex("BFInj").set("name", "v" + i).set("vector", createDeterministicVector(i, DIMENSIONS)).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("BFInj[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();

    // Capture the ordinal->vectorId snapshot the engine is currently using.
    final int[] ordinalA = ((int[]) readField(index, "ordinalToVectorId")).clone();
    assertThat(ordinalA.length).isGreaterThanOrEqualTo(2);
    final VectorLocationIndex vectorIndex = readField(index, "vectorIndex");

    // Build the vectors view from snapshot A (exactly as findNeighborsFromVector does).
    final RandomAccessVectorValues vectorsFromA = new ArcadePageVectorValues((DatabaseInternal) database, DIMENSIONS,
        "vector", vectorIndex, ordinalA, index);

    // Probe ordinal whose mapping differs between A and its reverse, so the bug (if present) is visible.
    final int probeOrdinal = 0;
    final VectorFloat<?> query = vectorsFromA.getVector(probeOrdinal);
    final VectorLocationIndex.VectorLocation expectedLoc = vectorIndex.getLocation(ordinalA[probeOrdinal]);
    final RID expectedRid = expectedLoc.rid;

    // Simulate a concurrent rebuild reassigning the field to a different permutation AFTER vectorsFromA
    // was built. The reverse permutation guarantees ordinalA[0] != ordinalB[0].
    final int[] ordinalB = new int[ordinalA.length];
    for (int i = 0; i < ordinalA.length; i++)
      ordinalB[i] = ordinalA[ordinalA.length - 1 - i];
    assertThat(ordinalB[probeOrdinal]).isNotEqualTo(ordinalA[probeOrdinal]);
    writeField(index, "ordinalToVectorId", ordinalB);

    try {
      final List<Pair<RID, Float>> results = new ArrayList<>();
      final Method bruteForceScan = LSMVectorIndex.class.getDeclaredMethod("bruteForceScan",
          VectorFloat.class, int.class, Set.class, List.class, RandomAccessVectorValues.class, int[].class);
      bruteForceScan.setAccessible(true);
      // Pass snapshot A together with vectorsFromA - they must stay consistent regardless of the live field.
      bruteForceScan.invoke(index, query, 5, null, results, vectorsFromA, ordinalA);

      assertThat(results).isNotEmpty();
      final Pair<RID, Float> top = results.getFirst();
      assertThat(top.getSecond())
          .as("Self-match must score ~0 distance")
          .isLessThan(1e-3f);
      assertThat(top.getFirst().getBucketId())
          .as("The ~0 self-match score must be paired with the RID from the SAME snapshot (issue #4581)")
          .isEqualTo(expectedRid.getBucketId());
      assertThat(top.getFirst().getPosition())
          .as("The ~0 self-match score must be paired with the RID from the SAME snapshot (issue #4581)")
          .isEqualTo(expectedRid.getPosition());
    } finally {
      // Restore the real mapping so index teardown is consistent.
      writeField(index, "ordinalToVectorId", ordinalA);
    }
  }

  private List<Result> executeVectorSearch(final float[] queryVector, final int k) {
    final StringBuilder vectorStr = new StringBuilder("[");
    for (int i = 0; i < queryVector.length; i++) {
      if (i > 0)
        vectorStr.append(",");
      vectorStr.append(queryVector[i]);
    }
    vectorStr.append("]");

    final String sql = String.format(
        "SELECT distance, record FROM (SELECT expand(vectorNeighbors('BFVec[vector]', %s, %d)))",
        vectorStr, k);

    final List<Result> results = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        results.add(rs.next());
    }
    return results;
  }

  /**
   * Creates a deterministic, unique, non-zero vector (required for COSINE similarity).
   */
  private static float[] createDeterministicVector(final int index, final int dimensions) {
    final float[] vector = new float[dimensions];
    for (int j = 0; j < dimensions; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final Object target, final String name) throws Exception {
    final Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return (T) f.get(target);
  }

  private static void writeField(final Object target, final String name, final Object value) throws Exception {
    final Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }

  /**
   * Captures messages at WARNING and above while still forwarding them to the production logger.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;
    private final Logger       delegate;

    CapturingLogger(final List<String> captured, final Logger delegate) {
      this.captured = captured;
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
          // Fall back to the raw template; good enough for substring matching.
        }
      }
      captured.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
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
