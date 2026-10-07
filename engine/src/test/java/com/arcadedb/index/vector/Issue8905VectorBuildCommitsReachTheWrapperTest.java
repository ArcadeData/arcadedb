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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8905: two gaps of {@code LSMVectorIndex.build()} on a replicated database.
 * <ol>
 *   <li>PHASE 3, {@code buildGraphWithChunking()}, committed its graph persist chunks on the inner
 *   {@link LocalDatabase} while the rest of {@code build()} commits through the wrapper. On an HA node that wrapper is
 *   the Raft-replicated database, so those chunks - and whatever bulk-loaded vector pages the build's transaction held
 *   at that point - landed on the building node's disk only. The graph file is node-local (every node persists its own
 *   graph, #8292), so routing those commits through the wrapper is no answer either: the graph pages have different
 *   versions on every node. PHASE 3 is now skipped on a replicated database, and the graph is built node-locally on
 *   first use, as every other node of the cluster builds it.</li>
 *   <li>The build's chunk size ({@code arcadedb.index.buildChunkSizeMB}, 50MB by default) was not bounded by the
 *   replicated entry cap ({@code GlobalConfiguration.maxReplicatedRaftEntrySize}, 32MB by default), and on the ordinary arm one chunk is one replicated entry.</li>
 * </ol>
 * The wrapper is a delegating proxy installed the way the HA plugin installs {@code RaftReplicatedDatabase}
 * ({@link LocalDatabase#setWrappedDatabaseInstance}); it counts the {@code commit()} calls it receives and can claim to
 * be replicated. The end-to-end behaviour on a real cluster is covered by {@code Issue8905VectorBuildHATest} in the
 * ha-raft module.
 */
class Issue8905VectorBuildCommitsReachTheWrapperTest extends TestHelper {

  // TWO DIMENSIONS ARE ESTIMATED AT 2 * 4 + 32 = 40 BYTES A VECTOR, SO 20_000 OF THEM ARE ~780KB: UNDER THE 1MB CHUNK
  // SET BELOW, SO THE BULK LOAD COMMITS NO CHUNK AND THE GRAPH STAYS UNLOADED (PHASE 3 IS ONLY REACHED THEN). THE GRAPH
  // OF 20_000 NODES, WITH ITS VECTORS INLINE, IS WELL PAST 1MB, SO ITS PERSIST CROSSES AT LEAST ONE CHUNK BOUNDARY
  private static final int    GRAPH_RECORDS    = 20_000;
  private static final int    GRAPH_DIMENSIONS = 2;
  private static final String GRAPH_TYPE       = "Issue8905Graph";

  // 3_000 VECTORS OF 32 DIMENSIONS ARE ESTIMATED AT 3_000 * 160 = ~470KB: ONE CHUNK AT THE 50MB DEFAULT
  private static final int    CAPPED_RECORDS    = 3_000;
  private static final int    CAPPED_DIMENSIONS = 32;
  private static final String CAPPED_TYPE       = "Issue8905Capped";

  @AfterEach
  void resetConfiguration() {
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.reset();
    GlobalConfiguration.HA_APPEND_BUFFER_SIZE.reset();
  }

  /**
   * Finding 1. The index is reopened from disk so its graph is not loaded, and the bulk load fits one chunk so nothing
   * loads it before PHASE 3: the only path into {@code buildGraphWithChunking()}, whose persist would cross a chunk
   * boundary and commit the build's transaction on the inner instance. On a replicated database the build has to skip
   * it, so its whole transaction - one chunk here - commits once, through the wrapper. The index still answers a search
   * afterwards: its graph is built on first use.
   */
  @Test
  void aReplicatedBuildCommitsOnlyThroughTheWrapperAndLeavesTheGraphToTheNode() {
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
    createVectorType(GRAPH_TYPE, GRAPH_RECORDS, GRAPH_DIMENSIONS, true);

    reopenDatabase();
    final LSMVectorIndex index = bucketIndex(GRAPH_TYPE);

    final AtomicInteger wrapperCommits = new AtomicInteger();
    final AtomicBoolean graphBuilt = new AtomicBoolean();
    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, true));
    try {
      index.build(null, (phase, processed, total, insertsInProgress) -> graphBuilt.set(true));
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(graphBuilt.get()).as("PHASE 3 must not run on a replicated database").isFalse();
    assertThat(wrapperCommits.get()).as("the build's one chunk commits once, through the wrapper").isEqualTo(1);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, 5)).hasSize(5);
  }

  /**
   * A guard for the same path off HA: a database that is not replicated still builds and persists the graph inside
   * the build, in chunks. The skip must cost a single node nothing.
   */
  @Test
  void aBuildThatIsNotReplicatedStillPersistsTheGraphInChunks() {
    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
    createVectorType(GRAPH_TYPE, GRAPH_RECORDS, GRAPH_DIMENSIONS, true);

    reopenDatabase();
    final LSMVectorIndex index = bucketIndex(GRAPH_TYPE);

    final AtomicBoolean graphBuilt = new AtomicBoolean();
    index.build(null, (phase, processed, total, insertsInProgress) -> graphBuilt.set(true));

    assertThat(graphBuilt.get()).as("the build has to reach PHASE 3, or this proves nothing").isTrue();
    assertThat(index.getGraphFile().getLastWrittenGraphBytes())
        .as("the graph is past the 1MB chunk, so its persist crossed a chunk boundary").isGreaterThan(1024L * 1024);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, 5)).hasSize(5);
  }

  /**
   * Finding 2. A build over ~470KB of estimated vectors fits one chunk at the 50MB default. On a database whose wrapper
   * is replicated with a 256KB entry cap, the chunk is clamped below that cap, so the same build commits more than
   * once.
   */
  @Test
  void aReplicatedBuildChunksBelowTheReplicatedEntryCap() {
    createVectorType(CAPPED_TYPE, 0, CAPPED_DIMENSIONS, false);
    createRecords(CAPPED_TYPE, CAPPED_RECORDS, CAPPED_DIMENSIONS);
    final LSMVectorIndex index = bucketIndex(CAPPED_TYPE);

    GlobalConfiguration.HA_APPEND_BUFFER_SIZE.setValue("256KB");

    final AtomicInteger wrapperCommits = new AtomicInteger();
    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, true));
    try {
      assertThat(index.build(null, null)).isEqualTo(CAPPED_RECORDS);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    // HALF OF THE 256KB CAP IS 128KB OF ESTIMATED VECTORS PER CHUNK: ~470KB IS THREE OF THEM, AND THE FINAL COMMIT
    assertThat(wrapperCommits.get()).as("the build has to commit in chunks below the replicated entry cap")
        .isGreaterThanOrEqualTo(4);
  }

  /**
   * A guard: the same build on a database that is not replicated keeps the configured chunk size, whatever the HA
   * settings say. The clamp costs a replicated database Raft round trips; it must cost a single node nothing.
   */
  @Test
  void aBuildThatIsNotReplicatedKeepsTheConfiguredChunk() {
    createVectorType(CAPPED_TYPE, 0, CAPPED_DIMENSIONS, false);
    createRecords(CAPPED_TYPE, CAPPED_RECORDS, CAPPED_DIMENSIONS);
    final LSMVectorIndex index = bucketIndex(CAPPED_TYPE);

    GlobalConfiguration.HA_APPEND_BUFFER_SIZE.setValue("256KB");

    final AtomicInteger wrapperCommits = new AtomicInteger();
    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, false));
    try {
      assertThat(index.build(null, null)).isEqualTo(CAPPED_RECORDS);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(wrapperCommits.get()).as("one chunk at the 50MB default: the final commit only").isEqualTo(1);
  }

  private void createVectorType(final String typeName, final int records, final int dimensions,
      final boolean storeVectorsInGraph) {
    final VertexType type = database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("vector", float[].class);
    createRecords(typeName, records, dimensions);

    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(typeName, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(dimensions);
    builder.withStoreVectorsInGraph(storeVectorsInGraph);
    builder.create();
  }

  private void createRecords(final String typeName, final int records, final int dimensions) {
    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = ((i * 31 + j * 7) % 1_009 + 1) / 1_009f;
        database.newVertex(typeName).set("vector", vector).save();
      }
    });
  }

  private LSMVectorIndex bucketIndex(final String typeName) {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(typeName + "[vector]");
    return (LSMVectorIndex) index.getIndexesOnBuckets()[0];
  }

  private static DatabaseInternal countingWrapper(final DatabaseInternal delegate, final AtomicInteger commits,
      final boolean replicated) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("commit".equals(method.getName()) && (args == null || args.length == 0))
            commits.incrementAndGet();
          else if ("isReplicated".equals(method.getName()) && (args == null || args.length == 0))
            return replicated;
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }
}
