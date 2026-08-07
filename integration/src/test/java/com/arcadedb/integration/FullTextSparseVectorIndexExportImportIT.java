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
package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5650: {@code IMPORT DATABASE} from a JSONL dump rebuilds a {@code FULL_TEXT} or
 * {@code LSM_SPARSE_VECTOR} index through {@code getOrCreateTypeIndex()}, which carries no metadata, so every
 * setting that lives entirely in the index's metadata - analyzer/BM25 tuning/per-field boosts for FULL_TEXT,
 * dimensions/modifier/weightQuantization for LSM_SPARSE_VECTOR - is silently dropped on restore even though the
 * export (via {@code writeToJSON}/{@code toJSON}) carries all of it. Mirrors {@link VectorIndexExportImportIT},
 * which pins the same contract for LSM_VECTOR (issue #5069/#5639): every setting away from its default, so a knob
 * lost in the round trip fails the assertion instead of silently reverting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextSparseVectorIndexExportImportIT {

  private final static String SOURCE_PATH  = "target/databases/fulltext-sparse-export-source";
  private final static String RESTORE_PATH = "target/databases/fulltext-sparse-export-restore";
  private final static String FILE         = "target/fulltext-sparse-export.jsonl.tgz";
  private final static int    DIMENSIONS   = 64;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PATH));
    FileUtils.deleteRecursively(new File(FILE));
  }

  @Test
  void exportAndImportDatabaseWithFullTextAndSparseVectorIndexes() throws Exception {
    // 1. Create a source database with a tuned FULL_TEXT index and a tuned LSM_SPARSE_VECTOR index, and some data
    try (final Database db = new DatabaseFactory(SOURCE_PATH).create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType("Article");
        db.getSchema().getType("Article").createProperty("title", Type.STRING);
        db.getSchema().getType("Article").createProperty("text", Type.STRING);
        // Every setting away from its default, so a knob lost in the round trip fails here instead of silently
        // reverting: BM25 with tuned k1/b would pass whether or not it was restored, so also tune a per-field
        // boost, which changes ranking observably.
        db.command("sql", "CREATE INDEX ON Article (title, text) FULL_TEXT METADATA "
            + "{similarity: 'BM25', bm25_k1: 1.7, bm25_b: 0.55, defaultOperator: 'AND', "
            + "title_boost: 3.5, allowLeadingWildcard: true}");

        db.getSchema().createDocumentType("Sparse");
        db.getSchema().getType("Sparse").createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        db.getSchema().getType("Sparse").createProperty("weights", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA "
            + "{dimensions: " + DIMENSIONS + ", modifier: 'IDF', weightQuantization: 'FP16'}");
      });

      db.transaction(() -> {
        db.command("sql", "INSERT INTO Article SET title = 'rare quantum finding', text = 'quantum data recorded'");
        for (int i = 0; i < 5; i++)
          db.command("sql", "INSERT INTO Article SET title = 'common report " + i + "', text = 'ordinary data record'");
      });

      db.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final int[] tokens = new int[] { i, i + 1, i + 2 };
          final float[] weights = new float[] { 0.5f, 0.25f, 0.1f };
          final MutableDocument doc = db.newDocument("Sparse");
          doc.set("tokens", tokens);
          doc.set("weights", weights);
          doc.save();
        }
      });

      assertThat(db.countType("Article", true)).isEqualTo(6);
      assertThat(db.countType("Sparse", true)).isEqualTo(5);

      // Validate and PERSIST the BM25 corpus counters before export: an ordinary insert updates the live counters
      // but never saves schema.json (persistence is deferred - see LSMTreeFullTextIndex.ensureCounters()), so
      // without this step the exported counters would still be the index's creation-time (0, valid) snapshot and
      // could never exercise the doubling this test targets. REBUILD INDEX ... WITH statsOnly = true is the
      // documented way an operator gets a source database into this (realistic, e.g. post-bulk-load) state.
      db.command("sql", "REBUILD INDEX `Article[title,text]` WITH statsOnly = true");
      final FullTextIndexMetadata srcFtMetadata = ((LSMTreeFullTextIndex) ((TypeIndex) db.getSchema()
          .getIndexByName("Article[title,text]")).getIndexesOnBuckets()[0]).getFullTextMetadata();
      assertThat(srcFtMetadata.getTotalDocs()).as("source corpus counters must be validated before export").isEqualTo(6L);
    }

    // 2. Export it to JSONL
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    // 3. Import the dump into a fresh database
    try (final Database db2 = new DatabaseFactory(RESTORE_PATH).create()) {
      db2.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    // 4. Verify the restored database has the schema, the two indexes with their full metadata, and the data
    try (final Database db2 = new DatabaseFactory(RESTORE_PATH).open()) {
      final Schema schema = db2.getSchema();

      assertThat(schema.getType("Article")).isNotNull();
      assertThat(db2.countType("Article", true)).isEqualTo(6);
      assertThat(schema.getType("Sparse")).isNotNull();
      assertThat(db2.countType("Sparse", true)).isEqualTo(5);

      // --- FULL_TEXT: structural (unique/nullStrategy survive already) + the metadata that was previously dropped
      final Index fullTextIndex = schema.getIndexByName("Article[title,text]");
      assertThat(fullTextIndex).as("Full-text index should be restored").isNotNull();
      assertThat(fullTextIndex.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      final FullTextIndexMetadata ftMetadata = ((LSMTreeFullTextIndex) ((TypeIndex) fullTextIndex).getIndexesOnBuckets()[0])
          .getFullTextMetadata();
      assertThat(ftMetadata.getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
      assertThat(ftMetadata.getBm25K1()).isEqualTo(1.7f);
      assertThat(ftMetadata.getBm25B()).isEqualTo(0.55f);
      assertThat(ftMetadata.getDefaultOperator()).isEqualTo("AND");
      assertThat(ftMetadata.getFieldBoost("title")).isEqualTo(3.5f);
      assertThat(ftMetadata.isAllowLeadingWildcard()).isTrue();

      // The BM25 corpus counters must describe the TARGET's own replayed documents, not the source database's
      // validated count carried through restore and then doubled by replay (review finding on PR #5936):
      // withPersistedMetadata restores the source's counters onto an index that is then repopulated by replaying
      // every document, so without an explicit reset the counters end up at 2x the actual imported count.
      assertThat(ftMetadata.getTotalDocs()).as("BM25 totalDocs must match the imported document count, not be doubled")
          .isEqualTo(db2.countType("Article", true));

      // The restored index must be searchable with the restored AND operator and per-field boost still ranking
      final ResultSet rs = db2.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[title,text]', 'quantum data') = true");
      assertThat(rs.hasNext()).as("Full-text search should return results after import").isTrue();
      float rareScore = -1f;
      while (rs.hasNext()) {
        final Result r = rs.next();
        if ("rare quantum finding".equals(r.getProperty("title")))
          rareScore = ((Number) r.getProperty("$score")).floatValue();
      }
      assertThat(rareScore).as("The document matching both terms must be found").isGreaterThan(0f);

      // --- LSM_SPARSE_VECTOR: structural + the metadata that was previously dropped
      final Index sparseIndex = schema.getIndexByName("Sparse[tokens,weights]");
      assertThat(sparseIndex).as("Sparse vector index should be restored").isNotNull();
      assertThat(sparseIndex.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);

      final LSMSparseVectorIndexMetadata sparseMetadata = ((LSMSparseVectorIndex) ((TypeIndex) sparseIndex).getIndexesOnBuckets()[0])
          .getSparseMetadata();
      assertThat(sparseMetadata.dimensions).isEqualTo(DIMENSIONS);
      assertThat(sparseMetadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
      assertThat(sparseMetadata.weightQuantization).isEqualTo(WeightQuantization.FP16);

      // The restored index must be populated with all imported postings (rebuilt via the put hook)
      assertThat(sparseIndex.countEntries()).as("Sparse vector index should have postings for all imported records").isGreaterThan(0);
    }
  }
}
