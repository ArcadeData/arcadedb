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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #7058: a similarity search issued inside an explicit transaction, after writes to the
 * index, rebuilds the graph synchronously on the calling thread, and the persist step of that rebuild used to commit
 * whatever transaction the thread held - the caller's - and to switch it to WAL-less first. Over Bolt that surfaced
 * as the client's own COMMIT failing with {@code Neo.ClientError.Transaction.TransactionNotFound: Transaction not
 * begun}. The persist now runs in a transaction of its own, nested when the caller holds one, and leaves the
 * caller's untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7058SearchInsideTransactionTest {
  private static final String DB_PATH     = "./target/databases/Issue7058SearchInsideTransactionTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 50;

  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void searchInsideAnExplicitTransactionLeavesTheTransactionOpenAndUntouched() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random rng = new Random(7058);

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        // Keep the background rebuild machinery out of the picture: the rebuild under test is the synchronous one
        // the first search after a batch of writes performs on the calling thread.
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

        db.transaction(() -> {
          final DocumentType t = db.getSchema().createDocumentType("Doc");
          t.createProperty("id", Type.INTEGER);
          t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
            + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

        db.transaction(() -> {
          for (int i = 0; i < NUM_VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
        });

        final LSMVectorIndex lsm = vectorIndex(db);
        assertThat(lsm.getStats().get("graphNodeCount")).as("precondition: no graph built yet, so the search below rebuilds it")
            .isEqualTo(0L);

        db.begin();
        // Uncommitted work in the caller's transaction, the same shape as the reporter's MERGE before the search.
        db.newDocument("Doc").set("id", NUM_VECTORS).set("embedding", randomVector(rng)).save();
        final boolean useWAL = ((DatabaseInternal) db).getTransaction().isUseWAL();

        assertThat(lsm.findNeighborsFromVector(randomVector(new Random(3)), 5, 64)).isNotEmpty();

        assertThat(db.isTransactionActive())
            .as("the graph rebuild the search triggered must not commit the caller's transaction")
            .isTrue();
        assertThat(((DatabaseInternal) db).getTransaction().isUseWAL())
            .as("the graph rebuild must not switch the caller's transaction to WAL-less either")
            .isEqualTo(useWAL);
        assertThatCode(db::commit).as("the caller's COMMIT must still find its transaction").doesNotThrowAnyException();

        assertThat(db.countType("Doc", true)).isEqualTo(NUM_VECTORS + 1);
        assertThat(lsm.getStats().get("graphNodeCount"))
            .as("the graph must have been built and persisted all the same, in a transaction of its own")
            .isEqualTo((long) NUM_VECTORS);
      } finally {
        db.drop();
      }
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
