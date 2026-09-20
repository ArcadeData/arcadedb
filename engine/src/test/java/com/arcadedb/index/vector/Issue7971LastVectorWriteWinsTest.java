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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7971: when one transaction rewrites the same record's embedding more than once, the
 * commit kept EVERY rewrite as a live vector of that record, and which of them a search ranked the record by was
 * decided by {@code ComparableVector} order - a hash of the vector's contents - rather than by which write ran last.
 * <p>
 * Two things caused it. {@code LSMVectorIndex.remove()} queued an all-zero placeholder key because it "had no vector
 * to queue", so the {@code REMOVE} never landed on the same {@code ComparableKey} as the {@code ADD} it was meant to
 * retire and {@code TransactionIndexContext}'s per-key dedup never collapsed the pair; and the commit replay walked
 * the resulting {@code TreeMap} in KEY order, with nothing on an entry to say when it had been written. A dense
 * vector index holds one live vector per RID, so a record re-embedded twice inside a transaction came out of it
 * indexed under both embeddings at once - three live vectors after two rewrites, three graph nodes for one record,
 * and the same record returned several times by one search.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7971LastVectorWriteWinsTest {
  private static final String DB_PATH    = "./target/databases/Issue7971LastVectorWriteWinsTest";
  private static final int    DIMENSIONS = 8;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void rewritingAnEmbeddingTwiceLeavesOneVectorBehind() {
    withDatabase(db -> {
      final RID rid = insertWith(db, unit(0));

      // One transaction, three embeddings for the same record. Only the LAST one is what the record holds when the
      // transaction commits, so the index may hold exactly one vector for it - the last one.
      db.transaction(() -> {
        final MutableDocument doc = db.lookupByRID(rid, true).asDocument(true).modify();
        doc.set("embedding", unit(1)).save();
        doc.set("embedding", unit(2)).save();
        doc.set("embedding", unit(3)).save();
      });

      assertOneVectorPerRecord(db, rid);
    });
  }

  /**
   * The same rewrite sequence on a record CREATED inside the very transaction that rewrites it: the first
   * {@code ADD} has no committed vector behind it, which is the shape the re-embedding pipelines of issue #7971
   * actually run.
   */
  @Test
  void rewritingAnEmbeddingOfARecordCreatedInTheSameTransactionLeavesOneVectorBehind() {
    withDatabase(db -> {
      final RID[] holder = new RID[1];
      db.transaction(() -> {
        final MutableDocument doc = db.newDocument("Doc").set("embedding", unit(1));
        doc.save();
        doc.set("embedding", unit(2)).save();
        doc.set("embedding", unit(3)).save();
        holder[0] = doc.getIdentity();
      });

      assertOneVectorPerRecord(db, holder[0]);
    });
  }

  /**
   * The same guarantee without {@code DocumentIndexer} in the middle: two {@code put()}s for one RID inside one
   * transaction, with no {@code remove()} between them to coalesce with. Nothing but the write order the commit
   * replay carries can tell those two apart, so this is what pins it.
   */
  @Test
  void twoDirectPutsForOneRecordCommitAsOneVector() {
    withDatabase(db -> {
      final RID rid = insertWith(db, unit(0));
      final LSMVectorIndex idx = vectorIndex(db);

      db.transaction(() -> {
        idx.remove(new Object[] { unit(0) }, rid);
        idx.put(new Object[] { unit(1) }, new RID[] { rid });
        idx.put(new Object[] { unit(2) }, new RID[] { rid });
      });

      assertOneVectorPerRecord(db, rid);
    });
  }

  /**
   * What "one vector per record" means everywhere it is observable: the live count of the index, the number of nodes
   * a graph build produces, and how many times one search hands the record back.
   * <p>
   * Deliberately NOT asserted here: WHICH embedding a search scores the record at. The delta scan resolves a pending
   * RID's vector through the record when the payload is not resident, so a query that happens to be the record's
   * current property value scores 0 whatever the index holds - an assertion on the distance would pass on the broken
   * engine too.
   */
  private static void assertOneVectorPerRecord(final Database db, final RID rid) {
    final LSMVectorIndex idx = vectorIndex(db);

    assertThat(idx.countEntries())
        .as("a record carries one embedding, so the index must hold one live vector for it, not one per rewrite")
        .isEqualTo(1L);

    final List<Pair<RID, Float>> results = idx.findNeighborsFromVector(unit(0), 10);
    assertThat(results.stream().filter(r -> rid.equals(r.getFirst())).count())
        .as("one search must return the record once, not once per embedding it held during the transaction")
        .isEqualTo(1L);

    idx.buildVectorGraphNow();
    assertThat(idx.getStats().get("graphNodeCount"))
        .as("the graph must carry one node for the record, not one per embedding it held during the transaction")
        .isEqualTo(1L);
  }

  private static void withDatabase(final java.util.function.Consumer<Database> body) {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        // The absorb machinery is irrelevant here: what is under test is what the COMMIT leaves in the index.
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
            + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

        body.accept(db);
      } finally {
        db.drop();
      }
    }
  }

  private static RID insertWith(final Database db, final float[] vector) {
    final RID[] holder = new RID[1];
    db.transaction(() -> {
      final MutableDocument doc = db.newDocument("Doc").set("embedding", vector);
      doc.save();
      holder[0] = doc.getIdentity();
    });
    return holder[0];
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  /** A unit vector along axis {@code axis}: any two of them are the same distance apart. */
  private static float[] unit(final int axis) {
    final float[] v = new float[DIMENSIONS];
    v[axis] = 1f;
    return v;
  }
}
