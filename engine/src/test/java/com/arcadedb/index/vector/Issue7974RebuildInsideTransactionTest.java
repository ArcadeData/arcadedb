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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7974: a similarity search issued inside an open transaction rebuilds the graph
 * synchronously when the resident graph is small, and everything that rebuild read used to come back through the
 * calling thread's transaction. An embedding written and not yet committed was therefore baked into the state the
 * index keeps, and the rollback could not take it back: the record reverted, the index did not, and it was left
 * describing a row by a vector no committed transaction ever wrote.
 * <p>
 * The two tests pin the two places that state lives, because they fail independently: the index-scoped vector cache
 * every later search on this session reads, and - once the index stores its vectors inline - the persisted graph
 * file, which survives a reopen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7974RebuildInsideTransactionTest {
  private static final String DB_PATH     = "./target/databases/Issue7974RebuildInsideTransactionTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 40;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aRolledBackEmbeddingMustNotSurviveInTheIndexScopedVectorCache() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7974);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        createSchema(db, false);

        final RID[] rids = insertSeedRows(db, rng);
        final LSMVectorIndex lsm = vectorIndex(db);

        // First search builds the graph over the committed rows.
        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
        assertThat(lsm.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

        // One more row, committed, so it sits in the delta buffer and leaves a mutation behind: that is what makes
        // the search inside the transaction below rebuild rather than just walk the graph.
        final float[] committedVector = randomVector(rng);
        final RID victim = insertRow(db, "buffered", committedVector);

        final float[] rolledBackVector = farAwayVector();

        db.begin();
        db.lookupByRID(victim, true).asDocument().modify().set("embedding", rolledBackVector).save();
        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
        db.rollback();

        final Document afterRollback = db.lookupByRID(victim, true).asDocument();
        assertThat((float[]) afterRollback.get("embedding"))
            .as("precondition: the rollback took the record's embedding back")
            .containsExactly(committedVector);

        assertThat(ridsOf(lsm.findNeighborsFromVector(rolledBackVector, 5, 64)))
            .as("the index must not rank the row by an embedding no committed transaction ever wrote")
            .doesNotContain(victim);

        assertThat(ridsOf(lsm.findNeighborsFromVector(committedVector, 5, 64)))
            .as("the index must rank the row by the embedding the database actually holds")
            .contains(victim);

        assertThat(rids).hasSize(NUM_VECTORS);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aRolledBackEmbeddingMustNotSurviveInThePersistedGraph() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7974);

    final RID victim;
    final float[] committedVector;
    final float[] rolledBackVector = farAwayVector();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        // storeVectorsInGraph is what makes the damage outlive the session: the rebuild writes each vector inline
        // into the graph file, and every later search scores from the file rather than from the record.
        createSchema(db, true);

        final RID[] rids = insertSeedRows(db, rng);
        victim = rids[0];
        committedVector = (float[]) db.lookupByRID(victim, true).asDocument().get("embedding");

        final LSMVectorIndex lsm = vectorIndex(db);
        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
        assertThat(lsm.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

        // The row whose rebuild is triggered is NOT the row being rewritten here: this one only leaves a mutation
        // in the delta buffer, so the rebuild has to re-read the victim's vector from its document.
        insertRow(db, "buffered", randomVector(rng));

        db.begin();
        db.lookupByRID(victim, true).asDocument().modify().set("embedding", rolledBackVector).save();
        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
        db.rollback();
      } finally {
        db.close();
      }

      final Database reopened = factory.open();
      try {
        final LSMVectorIndex lsm = vectorIndex(reopened);

        assertThat((float[]) reopened.lookupByRID(victim, true).asDocument().get("embedding"))
            .as("precondition: the rollback took the record's embedding back")
            .containsExactly(committedVector);

        assertThat(ridsOf(lsm.findNeighborsFromVector(rolledBackVector, 5, 64)))
            .as("the persisted graph must not carry an embedding no committed transaction ever wrote")
            .doesNotContain(victim);

        assertThat(ridsOf(lsm.findNeighborsFromVector(committedVector, 5, 64)))
            .as("the persisted graph must carry the embedding the database actually holds")
            .contains(victim);
      } finally {
        reopened.drop();
      }
    }
  }

  @Test
  void rowsInsertedByARolledBackTransactionMustNotBeLeftInTheIndex() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7974);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        createSchema(db, false);

        insertSeedRows(db, rng);
        final LSMVectorIndex lsm = vectorIndex(db);
        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);

        // The mutation that makes the search inside the transaction rebuild rather than just walk the graph.
        insertRow(db, "buffered", randomVector(rng));

        // Enough uncommitted rows that the index pages describe well under the bucket's document count: that is
        // what sends the rebuild down its document-scan recovery fallback, which used to sweep the bucket and take
        // whatever it found there - rows this transaction had not committed included, each one handed a vector id
        // and a place in the graph that the rollback below could not take back.
        final float[] uncommittedVector = farAwayVector();

        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("name", "tx" + i).set("embedding", randomVector(rng)).save();
        db.newDocument("Doc").set("name", "txFar").set("embedding", uncommittedVector).save();

        lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
        db.rollback();

        assertThat(db.countType("Doc", true))
            .as("precondition: the rollback took the transaction's own rows back")
            .isEqualTo(NUM_VECTORS + 1);
        assertThat(lsm.getStats().get("totalVectors"))
            .as("a rolled back insert must leave nothing behind in the index")
            .isEqualTo((long) NUM_VECTORS + 1);
        assertThat(ridsOf(lsm.findNeighborsFromVector(uncommittedVector, 5, 64)))
            .as("no result may be a row no committed transaction ever wrote")
            .allSatisfy(rid -> assertThat(db.lookupByRID(rid, true).asDocument().getString("name"))
                .doesNotStartWith("tx"));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void compactingInsideATransactionIsStillRefused() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7974);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        createSchema(db, false);
        insertSeedRows(db, rng);

        final LSMVectorIndex lsm = vectorIndex(db);
        assertThat(lsm.scheduleCompaction()).as("precondition: the compaction slot was taken").isTrue();

        // A compaction rewrites the data file outside transactional control, so it may not run under a caller's
        // transaction. The refusal is asked BEFORE the build suspends that transaction (issue #7974) - inside the
        // suspension it could only ever answer false - so this pins the question at its new evaluation point.
        db.begin();
        try {
          assertThatThrownBy(lsm::compact)
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("inside a transaction");
        } finally {
          db.rollback();
        }

        // The refusal hands the scheduling slot back, so ask for it again before the control run.
        assertThat(lsm.scheduleCompaction()).isTrue();
        assertThat(lsm.compact()).as("and outside a transaction it runs").isTrue();
      } catch (final IOException | InterruptedException e) {
        throw new IllegalStateException(e);
      } finally {
        db.drop();
      }
    }
  }

  private static void createSchema(final Database db, final boolean storeVectorsInGraph) {
    // Keep the background rebuild machinery out of the picture: the rebuild under test is the synchronous one the
    // small-graph arm performs on the calling thread.
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    db.transaction(() -> {
      final DocumentType t = db.getSchema().createDocumentType("Doc");
      t.createProperty("name", Type.STRING);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
    });
    db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\", \"storeVectorsInGraph\": " + storeVectorsInGraph + " }");
  }

  private static RID[] insertSeedRows(final Database db, final Random rng) {
    final RID[] rids = new RID[NUM_VECTORS];
    db.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++)
        rids[i] = db.newDocument("Doc").set("name", "doc" + i).set("embedding", randomVector(rng)).save().getIdentity();
    });
    return rids;
  }

  private static RID insertRow(final Database db, final String name, final float[] vector) {
    final RID[] rid = new RID[1];
    db.transaction(() -> rid[0] = db.newDocument("Doc").set("name", name).set("embedding", vector).save().getIdentity());
    return rid[0];
  }

  private static List<RID> ridsOf(final List<Pair<RID, Float>> neighbors) {
    return neighbors.stream().map(Pair::getFirst).toList();
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
  }

  /** Far enough from every seed vector that only the row actually carrying it can rank first for it. */
  private static float[] farAwayVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = 1000f + i;
    return v;
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
