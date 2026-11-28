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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13484: a follower receives the graph a compaction rebuilt, but not the manifest and the ordinal map next to it. The
 * state is reproduced here on a single node by compacting and then dropping those two sidecars, which is what a follower holds
 * right after it applied the compaction. {@link LSMVectorIndex#certifyGraphShippedByReplicatedCompaction(Set)} must certify
 * that graph when the live set is still the rewrite, so that a later deletion reuses it instead of rebuilding, and must leave
 * every other shape to the ordinary load path.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
@Tag("vector")
class Issue13484CertifyShippedCompactionGraphTest {
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 1_500;
  private static final String DB_PATH     = "./target/databases/Issue13484CertifyShippedCompactionGraphTest";
  private static final String INDEX       = "Doc[embedding]";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Random random = new Random(13484);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      db.transaction(() -> {
        final DocumentType type = db.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
        type.createProperty("id", Type.INTEGER);
        type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("id", i, "embedding", randomVector(random)).save();
      });
      db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\" }");
      db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id < 200"));
      db.command("sql", "COMPACT INDEX `" + INDEX + "`");

      // What a follower holds once it applied the compaction: the graph pages, and nothing that says what they were built over
      final LSMVectorIndexGraphFile graphFile = piece(db).getGraphFile();
      assertThat(graphFile.hasPersistedGraph()).isTrue();
      graphFile.getManifest().invalidate();
      assertThat(graphFile.getManifest().exists()).isFalse();
      db.close();
    }
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theShippedGraphIsCertifiedAndReusedAfterADeletion() {
    final RID probe;
    final float[] query;
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      final LSMVectorIndex piece = piece(db);
      final int graphFileId = piece.getGraphFile().getFileId();

      assertThat(piece.certifyGraphShippedByReplicatedCompaction(Set.of(graphFileId + 1_000)))
          .as("a graph file the entry did not create is left alone").isFalse();
      assertThat(piece.certifyGraphShippedByReplicatedCompaction(Set.of(graphFileId))).isTrue();
      assertThat(piece.getGraphFile().getManifest().read().vectorCount()).isEqualTo(NUM_VECTORS - 200);
      assertThat(piece.certifyGraphShippedByReplicatedCompaction(Set.of(graphFileId)))
          .as("a certified graph is not certified twice").isFalse();

      db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id >= 200 AND id < 250"));
      final var probeDoc = db.query("sql", "SELECT FROM Doc WHERE id = 1000").next().toElement();
      probe = probeDoc.getIdentity();
      query = (float[]) probeDoc.get("embedding");
      db.close();
    }

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      final LSMVectorIndex piece = piece(db);
      assertThat(piece.getStats().get("deletedVectors")).isEqualTo(50L);

      final List<Pair<RID, Float>> neighbours = piece.findNeighborsFromVector(query, 10);
      assertThat(piece.getStats().get("graphRebuildCount")).as("the certified graph is reused despite the deletions").isZero();
      assertThat(neighbours.getFirst().getFirst()).isEqualTo(probe);
      db.close();
    }
  }

  @Test
  void aGraphWithDeletionsOnTopOfTheRewriteIsNotCertified() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      final LSMVectorIndex piece = piece(db);
      final int graphFileId = piece.getGraphFile().getFileId();
      db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id >= 200 AND id < 250"));

      assertThat(piece.certifyGraphShippedByReplicatedCompaction(Set.of(graphFileId)))
          .as("the live set is no longer the rewrite: ordinal i is not vector id i any more").isFalse();
      assertThat(piece.getGraphFile().getManifest().exists()).isFalse();
      db.close();
    }
  }

  private static LSMVectorIndex piece(final Database db) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
