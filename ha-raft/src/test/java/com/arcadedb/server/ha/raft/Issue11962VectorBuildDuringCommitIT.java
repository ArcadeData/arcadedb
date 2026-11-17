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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vector graph build that runs on the leader while another transaction is between its commit replay and its
 * publication (issue #11962). The replay puts the vector into the location index and the delta buffer, but its pages
 * and record are published only after the Raft round trip, so the build cannot read it and drops it like a deleted
 * vector. Its delta entry was trimmed with the ones the build incorporated, so once the commit completed the vector was
 * in neither the graph nor the delta, and no search found it until a later rebuild.
 */
class Issue11962VectorBuildDuringCommitIT extends BaseRaftHATest {
  private static final int DIMENSIONS = 16;
  private static final int BASE       = 30;

  private final Random random = new Random(11962);

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;
  }

  @Test
  void aVectorCommittedWhileABuildRanIsFoundOnceItsCommitCompletes() {
    assertFoundOnceTheCommitCompletes("Dense", 0);
  }

  /**
   * Most records carry no vector, so the pages yield fewer vectors than the bucket has records and the build cross-checks
   * them with a document scan. That scan cannot see the record in flight either, and it used to skip the recovery from
   * the in-memory locations, so the build also published a location index without the in-flight vector.
   */
  @Test
  void aVectorCommittedWhileABuildOfASparseIndexRanIsFoundOnceItsCommitCompletes() {
    assertFoundOnceTheCommitCompletes("Sparse", 2 * BASE);
  }

  private void assertFoundOnceTheCommitCompletes(final String typeName, final int recordsWithoutVector) {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database db = getServerDatabase(leaderIndex, getDatabaseName());

    db.transaction(() -> db.getSchema().createDocumentType(typeName, 1).createProperty("vector", Type.ARRAY_OF_FLOATS));
    db.command("sql", "CREATE INDEX ON " + typeName + " (vector) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
        + ", similarity: 'EUCLIDEAN', quantization: 'INT8'}");
    db.transaction(() -> {
      for (int i = 0; i < BASE; i++)
        db.newDocument(typeName).set("vector", vector()).save();
      for (int i = 0; i < recordsWithoutVector; i++)
        db.newDocument(typeName).set("name", "no vector " + i).save();
    });
    waitForAllServers();

    final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType(typeName).getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();

    // The build runs from another thread, as a search on another connection would, while the commit below waits between
    // its replay and its dispatch to Raft
    final AtomicBoolean built = new AtomicBoolean();
    final AtomicReference<Throwable> buildFailure = new AtomicReference<>();
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = name -> {
      if (!getDatabaseName().equals(name) || built.getAndSet(true))
        return;
      final Thread builder = new Thread(() -> {
        try {
          index.buildVectorGraphNow();
        } catch (final Throwable t) {
          buildFailure.set(t);
        }
      });
      builder.start();
      try {
        builder.join(60_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final float[] committed = vector();
    final RID[] rid = new RID[1];
    db.transaction(() -> rid[0] = db.newDocument(typeName).set("vector", committed).save().getIdentity());
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;

    assertThat(built.get()).as("precondition: a build ran inside the commit window").isTrue();
    assertThat(buildFailure.get()).as("precondition: the build completed").isNull();

    db.transaction(() -> assertThat(index.findNeighborsFromVector(committed, 1).stream().map(Pair::getFirst))
        .as("the vector committed while a graph build ran is found by its own vector (issue #11962)")
        .containsExactly(rid[0]));
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
