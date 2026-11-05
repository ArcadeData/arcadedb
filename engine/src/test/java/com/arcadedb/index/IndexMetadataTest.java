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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Cursor;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorEncoding;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Index metadata and index cursor lifecycle regression tests: the metadata keys persisted for each index kind, the
 * automatic flag of manually created indexes, and the release of index cursors left open by callers or by DML statements.
 * Each nested class keeps the fixture of the issue it was written for.
 */
class IndexMetadataTest {

  /**
   * Regression test for issue #5639: a {@code METADATA} key that {@code CREATE INDEX} accepts must either
   * reach the index or be reported. Two distinct failures are pinned here:
   * <ul>
   *   <li><b>unreachable settings</b> - {@code efSearch}, {@code inactivityRebuildTimeoutMs},
   *       {@code neighborOverflowFactor} and {@code alphaDiversityRelaxation} were read (or, for the last
   *       two, read and then lost one hop later) so an index created from SQL always ran with their
   *       defaults, and they did not survive a reopen either;</li>
   *   <li><b>dropped typos</b> - only the GEOSPATIAL builder rejected an unknown key (#5600), so
   *       {@code {"similarty": "EUCLIDEAN"}} built a COSINE index and reported success.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5639 index metadata keys")
  class IndexMetadataKeys extends TestHelper {

    private void createVectorType(final String typeName) {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".embedding ARRAY_OF_FLOATS");
      });
    }

    private LSMVectorIndexMetadata vectorMetadata(final String typeName) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(typeName + "[embedding]");
      return ((LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0]).getMetadata();
    }

    /**
     * The four dense-vector settings that the SQL clause could not reach. {@code efSearch} is the
     * search-time recall/latency knob and the other three drive the graph build, so this is a functional
     * gap and not only a diagnostics one.
     */
    @Test
    void everyDenseVectorSettingIsReachableFromTheMetadataClause() {
      createVectorType("Doc");

      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
            "dimensions": 8,
            "efSearch": 250,
            "inactivityRebuildTimeoutMs": 0,
            "neighborOverflowFactor": 1.4,
            "alphaDiversityRelaxation": 1.3
          }""");

      final LSMVectorIndexMetadata metadata = vectorMetadata("Doc");
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.inactivityRebuildTimeoutMs).isZero();
      assertThat(metadata.neighborOverflowFactor).isEqualTo(1.4f);
      assertThat(metadata.alphaDiversityRelaxation).isEqualTo(1.3f);
    }

    /**
     * A setting that only lives until the next restart is barely better than one that is dropped: the
     * persisted definition has to carry every knob {@code LSMVectorIndexMetadata.fromJSON()} reads.
     */
    @Test
    void denseVectorSettingsSurviveAReopen() {
      createVectorType("Doc");

      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
            "dimensions": 8,
            "similarity": "EUCLIDEAN",
            "efSearch": 250,
            "inactivityRebuildTimeoutMs": 7000,
            "neighborOverflowFactor": 1.4,
            "alphaDiversityRelaxation": 1.3,
            "mutationsBeforeRebuild": 4242,
            "pqClusters": 128,
            "pqCenterGlobally": false,
            "fusedPQ": true,
            "rerankOversample": 7
          }""");

      reopenDatabase();

      final LSMVectorIndexMetadata metadata = vectorMetadata("Doc");
      assertThat(metadata.dimensions).isEqualTo(8);
      assertThat(metadata.similarityFunction.name()).isEqualTo("EUCLIDEAN");
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.inactivityRebuildTimeoutMs).isEqualTo(7000);
      assertThat(metadata.neighborOverflowFactor).isEqualTo(1.4f);
      assertThat(metadata.alphaDiversityRelaxation).isEqualTo(1.3f);
      assertThat(metadata.mutationsBeforeRebuild).isEqualTo(4242);
      // The PQ knobs are only meaningful under PRODUCT quantization, but they are persisted unconditionally so that
      // "the persisted definition carries every setting" holds without an exception.
      assertThat(metadata.pqClusters).isEqualTo(128);
      assertThat(metadata.pqCenterGlobally).isFalse();
      assertThat(metadata.fusedPQ).isTrue();
      assertThat(metadata.rerankOversample).isEqualTo(7);
    }

    /**
     * The user-visible consequence of losing {@code similarity} on reload: the persisted definition names the metric
     * {@code similarityFunction} while the reader only looked for {@code similarity}, so every reopened index came back up
     * as COSINE and scored with the wrong metric. The fixture is a set of vectors on one ray - all at COSINE distance 0
     * from each other, all at different EUCLIDEAN distances - so a search that answers "everything is equally close"
     * pins the regression.
     */
    @Test
    void euclideanIndexKeepsItsMetricAfterAReopen() {
      createVectorType("Doc");
      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"EUCLIDEAN\"}");

      database.transaction(() -> {
        for (int i = 1; i <= 5; i++) {
          final float[] vector = new float[4];
          for (int j = 0; j < 4; j++)
            vector[j] = i * (j + 1) * 0.25f;
          database.newVertex("Doc").set("embedding", vector).save();
        }
      });

      reopenDatabase();

      assertThat(vectorMetadata("Doc").similarityFunction.name()).isEqualTo("EUCLIDEAN");

      final float[] query = new float[] { 0.25f, 0.5f, 0.75f, 1.0f };
      final ResultSet rs = database.query("sql",
          "SELECT distance FROM (SELECT expand(vectorNeighbors('Doc[embedding]', ?, 5)))", (Object) query);

      double maxDistance = 0;
      int rows = 0;
      while (rs.hasNext()) {
        maxDistance = Math.max(maxDistance, ((Number) rs.next().getProperty("distance")).doubleValue());
        ++rows;
      }
      assertThat(rows).isPositive();
      // Under COSINE every one of these vectors is at distance 0 from the query.
      assertThat(maxDistance).as("the reopened index must still score with EUCLIDEAN").isGreaterThan(0.1);
    }

    /**
     * A value of the wrong shape is a client mistake like any other, so it must answer HTTP 400. Reading the clause
     * straight off the JSON getters did not achieve that: the two float keys were cast to {@code Number}, so a string
     * raised a {@code ClassCastException}, and a JSON object raises {@code UnsupportedOperationException} - neither is in
     * the set the SQL layer turns into a parsing error, so both escaped as a 500.
     */
    @Test
    void malformedDenseVectorMetadataValuesAreClientErrors() {
      createVectorType("Doc");

      for (final String metadata : new String[] {
          "{\"dimensions\": 8, \"neighborOverflowFactor\": \"abc\"}",
          "{\"dimensions\": 8, \"alphaDiversityRelaxation\": \"abc\"}",
          "{\"dimensions\": 8, \"maxConnections\": {}}",
          "{\"dimensions\": 8, \"efSearch\": [1, 2]}",
          // A count read as 8 instead of 8.5 is a dropped input, not a rounded one.
          "{\"dimensions\": 8.5}",
          // getBoolean() answers false for any string that is not "true", so this used to DISABLE what was asked for.
          "{\"dimensions\": 8, \"addHierarchy\": \"yes\"}" })
        assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA " + metadata))
            .as("METADATA %s", metadata)
            .isInstanceOf(CommandSQLParsingException.class);

      assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();

      // An overflow is reported as an overflow: "must be a whole number" is true of 3000000000 and useless to whoever
      // wrote it.
      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"mutationsBeforeRebuild\": 3000000000}"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("must be between");

      // The quoted form of a number stays valid, on every numeric key and not just the ones read with getInt().
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
          + "{\"dimensions\": \"8\", \"neighborOverflowFactor\": \"1.4\", \"efSearch\": \"250\", \"addHierarchy\": \"true\"}");

      final LSMVectorIndexMetadata metadata = vectorMetadata("Doc");
      assertThat(metadata.dimensions).isEqualTo(8);
      assertThat(metadata.neighborOverflowFactor).isEqualTo(1.4f);
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.addHierarchy).isTrue();
    }

    /**
     * The Java builder and the SQL clause must reach the same set of settings: four of these had no fluent setter, so a
     * Java caller could not configure what SQL could.
     */
    @Test
    void everyDenseVectorSettingIsReachableFromTheJavaBuilder() {
      createVectorType("Doc");

      database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(8)
          .withEfSearch(250)
          .withInactivityRebuildTimeout(7000)
          .withMutationsBeforeRebuild(4242)
          .withGraphBuildCacheSize(2048)
          .withStoreVectorsInGraph(true)
          .create();

      final LSMVectorIndexMetadata metadata = vectorMetadata("Doc");
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.inactivityRebuildTimeoutMs).isEqualTo(7000);
      assertThat(metadata.mutationsBeforeRebuild).isEqualTo(4242);
      // locationCacheSize is deliberately absent: it is the one setting the builder no longer reaches, because it
      // cannot be honoured and a capped location index drops vectors (issues #5559 and #5568). See
      // Issue5559LocationCacheSizeTest for the refusal.
      assertThat(metadata.graphBuildCacheSize).isEqualTo(2048);
      assertThat(metadata.storeVectorsInGraph).isTrue();
    }

    /**
     * The bucket-level builder no longer keeps its own copy of the settings, so it has to carry them on the metadata its
     * constructor installs - including when {@code withMetadata()} is never called at all, which is what this path does.
     * Without that installation the settings would have nowhere to live before the factory reads them.
     */
    @Test
    void bucketLevelVectorBuilderCarriesItsSettings() {
      createVectorType("Doc");

      final String bucketName = database.getSchema().getType("Doc").getBuckets(false).get(0).getName();
      final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
          .buildBucketIndex("Doc", bucketName, new String[] { "embedding" })
          .withType(Schema.INDEX_TYPE.LSM_VECTOR);
      builder.withDimensions(8).withSimilarity("EUCLIDEAN").withEfSearch(250).withMaxConnections(24);

      final LSMVectorIndex index = (LSMVectorIndex) builder.create();
      assertThat(index.getMetadata().dimensions).isEqualTo(8);
      assertThat(index.getMetadata().similarityFunction.name()).isEqualTo("EUCLIDEAN");
      assertThat(index.getMetadata().efSearch).isEqualTo(250);
      assertThat(index.getMetadata().maxConnections).isEqualTo(24);
    }

    /**
     * The typed {@code withMetadata} overloads guard the cast instead of performing it, so a metadata of the wrong subtype
     * reports what was expected rather than raising a {@link ClassCastException} attributed to an unrelated line.
     */
    @Test
    void aMetadataOfTheWrongSubtypeIsReported() {
      createVectorType("Doc");

      final IndexMetadata plain = new IndexMetadata("Doc", new String[] { "embedding" }, -1);

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withMetadata(plain))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires LSMVectorIndexMetadata");

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
          .withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
          .withSparseVectorType()
          .withMetadata(plain))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires LSMSparseVectorIndexMetadata");
    }

    /**
     * Every fluent setter of the bucket-level builder, each with a distinct non-default value, asserted against the field
     * it is supposed to write. The setters are one-line delegations now that the builder keeps no fields of its own, and a
     * delegation that writes the neighbouring field - {@code withBeamWidth} landing on {@code maxConnections}, say - would
     * be invisible: the index would simply run with a setting nobody asked for, which is the failure this issue is about.
     */
    @Test
    void everyBucketBuilderSetterWritesItsOwnField() {
      createVectorType("Doc");

      final String bucketName = database.getSchema().getType("Doc").getBuckets(false).get(0).getName();
      final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
          .buildBucketIndex("Doc", bucketName, new String[] { "embedding" })
          .withType(Schema.INDEX_TYPE.LSM_VECTOR);

      builder.withDimensions(8)
          .withSimilarity("DOT_PRODUCT")
          .withMaxConnections(24)
          .withBeamWidth(150)
          .withEfSearch(250)
          .withNeighborOverflowFactor(1.4f)
          .withAlphaDiversityRelaxation(1.3f)
          .withIdProperty("docId")
          .withQuantization("PRODUCT")
          .withEncoding(VectorEncoding.FLOAT32)
          .withPQSubspaces(4)
          .withPQClusters(128)
          .withPQCenterGlobally(false)
          .withPQTrainingLimit(4096)
          .withFusedPQ(true)
          .withRerankOversample(7);

      final LSMVectorIndexMetadata metadata = builder.getVectorMetadata();
      assertThat(metadata.dimensions).isEqualTo(8);
      assertThat(metadata.similarityFunction.name()).isEqualTo("DOT_PRODUCT");
      assertThat(metadata.maxConnections).isEqualTo(24);
      assertThat(metadata.beamWidth).isEqualTo(150);
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.neighborOverflowFactor).isEqualTo(1.4f);
      assertThat(metadata.alphaDiversityRelaxation).isEqualTo(1.3f);
      assertThat(metadata.idPropertyName).isEqualTo("docId");
      assertThat(metadata.quantizationType.name()).isEqualTo("PRODUCT");
      assertThat(metadata.encoding).isEqualTo(VectorEncoding.FLOAT32);
      assertThat(metadata.pqSubspaces).isEqualTo(4);
      assertThat(metadata.pqClusters).isEqualTo(128);
      assertThat(metadata.pqCenterGlobally).isFalse();
      assertThat(metadata.pqTrainingLimit).isEqualTo(4096);
      assertThat(metadata.fusedPQ).isTrue();
      assertThat(metadata.rerankOversample).isEqualTo(7);

      // The string and enum overloads of the two paired setters must agree.
      assertThat(builder.withQuantization(VectorQuantizationType.INT8).getVectorMetadata().quantizationType)
          .isEqualTo(VectorQuantizationType.INT8);
      assertThat(builder.withEncoding("FLOAT32").getVectorMetadata().encoding).isEqualTo(VectorEncoding.FLOAT32);
    }

    @Test
    void unknownDenseVectorMetadataKeyIsReported() {
      createVectorType("Doc");

      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"similarty\": \"EUCLIDEAN\"}"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("similarty");

      assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
    }

    /**
     * {@code buildGraphNow} is consumed and removed by the SQL layer before the builder sees the clause, so
     * it must stay accepted even though the builder itself knows nothing about it.
     */
    @Test
    void buildGraphNowStaysAcceptedByTheMetadataClause() {
      createVectorType("Doc");

      assertThatCode(() -> database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"buildGraphNow\": false}"))
          .doesNotThrowAnyException();
      assertThat(database.getSchema().existsIndex("Doc[embedding]")).isTrue();
    }

    /**
     * The geospatial branch already rejected an unknown key (#5600); this pins that its errors reach the client as a 400
     * through the same three-exception catch as the other three index types, rather than only for the one exception its
     * setters happen to throw today.
     */
    @Test
    void malformedGeospatialMetadataIsReported() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Place");
        database.command("sql", "CREATE PROPERTY Place.location STRING");
      });

      for (final String metadata : new String[] { "{\"precison\": 6}", "{\"precision\": 6.9}", "{\"precision\": \"abc\"}",
          "{\"precision\": 99}", "{\"tokenization\": \"NOPE\"}" })
        assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Place (location) GEOSPATIAL METADATA " + metadata))
            .as("METADATA %s", metadata)
            .isInstanceOf(CommandSQLParsingException.class);

      assertThat(database.getSchema().existsIndex("Place[location]")).isFalse();
    }

    @Test
    void unknownSparseVectorMetadataKeyIsReported() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Sparse");
        database.command("sql", "CREATE PROPERTY Sparse.dims ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
      });

      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Sparse (dims, weights) LSM_SPARSE_VECTOR METADATA {\"modifer\": \"IDF\"}"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("modifer");

      assertThat(database.getSchema().existsIndex("Sparse[dims,weights]")).isFalse();

      // The keys the sparse index really has must keep working.
      database.command("sql", "CREATE INDEX ON Sparse (dims, weights) LSM_SPARSE_VECTOR METADATA "
          + "{\"dimensions\": 30000, \"modifier\": \"IDF\", \"weightQuantization\": \"FP16\"}");

      final LSMSparseVectorIndexMetadata metadata = ((LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("Sparse[dims,weights]")).getIndexesOnBuckets()[0]).getSparseMetadata();
      assertThat(metadata.dimensions).isEqualTo(30000);
      assertThat(metadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
    }

    @Test
    void unknownFullTextMetadataKeyIsReported() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Article");
        database.command("sql", "CREATE PROPERTY Article.text STRING");
      });

      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Article (text) FULL_TEXT METADATA {\"similarty\": \"BM25\"}"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("similarty");

      assertThat(database.getSchema().existsIndex("Article[text]")).isFalse();
    }

    /**
     * The full-text clause is the one with an open-ended key space: {@code <field>_analyzer} and
     * {@code <field>_boost} are per-property, so the guard must recognise them by shape.
     */
    @Test
    void fullTextPerFieldKeysStayAccepted() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Article");
        database.command("sql", "CREATE PROPERTY Article.text STRING");
        database.command("sql", "CREATE PROPERTY Article.title STRING");
      });

      database.command("sql", "CREATE INDEX ON Article (text, title) FULL_TEXT METADATA "
          + "{\"similarity\": \"BM25\", \"bm25_k1\": 1.4, \"bm25_b\": 0.6, \"defaultOperator\": \"AND\","
          + " \"title_analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\", \"title_boost\": 2.5}");

      final FullTextIndexMetadata metadata = fullTextMetadata("Article[text,title]");
      assertThat(metadata.getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
      assertThat(metadata.getBm25K1()).isEqualTo(1.4f);
      assertThat(metadata.getDefaultOperator()).isEqualTo("AND");
      assertThat(metadata.getAnalyzerClass("title")).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
      assertThat(metadata.getFieldBoost("title")).isEqualTo(2.5f);
    }

    /**
     * A METADATA clause that says nothing about {@code similarity} must not change the ranking model. The builder used to
     * hand the clause to {@code fromJSON()}, which reads a PERSISTED definition and takes a missing key as "written before
     * BM25 support": every index created with any METADATA silently dropped to CLASSIC scoring.
     */
    @Test
    void fullTextMetadataClauseKeepsTheBM25Default() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Article");
        database.command("sql", "CREATE PROPERTY Article.text STRING");
      });

      database.command("sql", "CREATE INDEX ON Article (text) FULL_TEXT METADATA {\"defaultOperator\": \"AND\"}");

      assertThat(fullTextMetadata("Article[text]").getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
    }

    /**
     * A per-field key is recognised by shape, so it is the one place a typo could still slip through the guard: an
     * analyzer or boost for a property the index does not cover is dead configuration, since only an indexed field is
     * analyzed and only an indexed field can produce a field-qualified match.
     */
    @Test
    void fullTextPerFieldKeyForANonIndexedPropertyIsReported() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Article");
        database.command("sql", "CREATE PROPERTY Article.text STRING");
        database.command("sql", "CREATE PROPERTY Article.title STRING");
      });

      // 'titel' is a typo for the indexed 'title'; 'text' is a real property but not covered by this index.
      for (final String metadata : new String[] { "{\"titel_boost\": 2.0}", "{\"titel_analyzer\": \"x\"}",
          "{\"text_boost\": 2.0}" })
        assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Article (title) FULL_TEXT METADATA " + metadata))
            .as("METADATA %s", metadata)
            .isInstanceOf(CommandSQLParsingException.class)
            .hasMessageContaining("indexed properties");

      assertThat(database.getSchema().existsIndex("Article[title]")).isFalse();
    }

    /**
     * The query parser understands only AND and OR, so any other operator was accepted and then behaved as OR - the last
     * value on the full-text clause that was taken and quietly ignored.
     */
    @Test
    void unknownFullTextDefaultOperatorIsReported() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Article");
        database.command("sql", "CREATE PROPERTY Article.text STRING");
      });

      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON Article (text) FULL_TEXT METADATA {\"defaultOperator\": \"XOR\"}"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("AND or OR");

      assertThat(database.getSchema().existsIndex("Article[text]")).isFalse();

      // The two it does understand keep working, case-insensitively.
      database.command("sql", "CREATE INDEX ON Article (text) FULL_TEXT METADATA {\"defaultOperator\": \"and\"}");
      assertThat(fullTextMetadata("Article[text]").getDefaultOperator()).isEqualTo("AND");
    }

    private FullTextIndexMetadata fullTextMetadata(final String indexName) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
      return ((LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0]).getFullTextMetadata();
    }
  }

  /**
   * #5662, the follow-ups left open by #5635.
   * <p>
   * The item this file spends most of its length on is the first one: nothing made an unclosed {@link IndexCursor}
   * visible. {@link Cursor} is now {@link AutoCloseable}, which is what makes an abandoned cursor a static-analysis
   * finding instead of something only a careful read can catch - but a compiler flag proves nothing at runtime, so the
   * tests here pin the two things that do: the call sites that were abandoning a cursor partway now release it, and a
   * cursor that is leaked anyway stops pinning its file forever once the GC gets to it.
   * <p>
   * The observable throughout is {@link LSMTreeIndexCompacted#countActiveCursors()}, the guard
   * {@code LSMTreeIndex.dropRetiredCompactedIndexes} consults before physically dropping a file a full compaction has
   * replaced. Every test that uses it first asserts it can actually rise above zero on this fixture, because a scan too
   * small to open a series cursor would make "it went back to zero" hold vacuously.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5662 index cursor follow-ups")
  class IndexCursorFollowUps extends TestHelper {
    private static final String TYPE_NAME = "CursorFollowUp";
    private static final int    RECORDS   = 600;

    @BeforeEach
    @Override
    public void beforeTest() {
      // explicit compaction only, so the sub-index appears exactly when this test asks for it
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // item 1 - nothing enforced close()
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void anIndexCursorIsUsableAsATryWithResourcesResource() throws Exception {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      // the point of the item: this compiles, which it did not before Cursor extended AutoCloseable. close() is also
      // declared without a checked exception, so the caller is not forced into a catch(Exception) it has nothing to do
      // with
      try (final IndexCursor cursor = bucketIndex().iterator(true)) {
        assertThat(cursor.hasNext()).isTrue();
        cursor.next();
        assertThat(compacted.countActiveCursors()).as("a live scan holds a series cursor over the compacted file").isPositive();
      }

      assertThat(compacted.countActiveCursors()).as("try-with-resources released the series cursors").isZero();
    }

    @Test
    void aSelectThatStopsAtTheFirstMatchReleasesItsIndexCursor() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      // exists() returns on the FIRST match, so the cursor is abandoned with most of the index still ahead of it: the
      // shape that used to leave a series cursor registered for the lifetime of the database
      final boolean found = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).exists();

      assertThat(found).as("the fixture must actually match, otherwise the scan never opened a cursor").isTrue();
      assertThat(compacted.countActiveCursors()).as("exists() must release the index cursor it abandoned partway").isZero();
    }

    @Test
    void aCountWithALimitReleasesItsIndexCursor() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      final long counted = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).limit(5).count();

      assertThat(counted).as("the limit must actually cut the scan short").isEqualTo(5);
      assertThat(compacted.countActiveCursors()).as("count() must release the index cursor it abandoned at the limit").isZero();
    }

    @Test
    void closingAPartiallyConsumedSelectIteratorReleasesItsIndexCursor() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      final var iterator = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).documents();
      assertThat(iterator.hasNext()).isTrue();
      iterator.next();
      assertThat(compacted.countActiveCursors()).as("the iterator is mid-scan, so a series cursor is open").isPositive();

      iterator.close();

      assertThat(compacted.countActiveCursors()).as("SelectIterator.close() must release the source index cursor").isZero();
    }

    /**
     * A {@code DELETE ... WHERE} puts the index scan in a SUB-plan and the {@code LIMIT} outside it, so the scan is
     * abandoned partway. Closing the result set used to close only the outer plan's steps - {@code SubQueryStep} never
     * closed the plan it wraps - and the scan inside it was released only if it happened to run to exhaustion.
     * <p>
     * #5681: {@code DeleteStatement.execute()} / {@code UpdateStatement.execute()} already drain the whole plan
     * eagerly, via {@code executeInternal()}, before ever handing the {@link ResultSet} back to the caller - so the
     * abandoned scan is now released the moment {@code executeInternal()} finishes, not only when the caller closes
     * the result set (which a {@code DELETE} run for its side effect - the common case - usually never does). The
     * explicit {@code close()} below therefore observes an already-released scan; what it still pins is that closing
     * the same {@code SubQueryStep}/sub-plan chain a second time - the shape a caller who DOES close its result set
     * exercises - stays a harmless no-op, matching the idempotent {@code close()} contract documented on
     * {@link LSMTreeIndexUnderlyingCompactedSeriesCursor}.
     */
    @Test
    void closingADeleteLimitedShortOfExhaustionReleasesTheScanInsideItsSubPlan() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      database.transaction(() -> {
        try (final ResultSet result = database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE value > 1 LIMIT 3")) {
          assertThat(((Number) result.next().getProperty("count")).intValue()).as("the LIMIT must cut the scan short")
              .isEqualTo(3);
          assertThat(compacted.countActiveCursors())
              .as("#5681: the scan inside the sub-plan is released as soon as the DELETE is done, before the caller ever"
                  + " gets a chance to close the result set")
              .isZero();
        }

        assertThat(compacted.countActiveCursors())
            .as("closing the result set reaches the scan inside the sub-plan too - here as a harmless second close()")
            .isZero();
      });
    }

    /**
     * The safety net behind the whole item: a cursor that IS leaked - because some call site nobody has read yet still
     * drops one - must stop blocking the retired-file drop once it becomes unreachable. It used to be counted, so a
     * single missed {@code close()} pinned the file with nothing left in the process that could ever release it.
     * <p>
     * This is the one test here that depends on the JVM rather than only on this codebase: it needs the collector to
     * actually clear a weak referent. That is reliable on HotSpot for a genuinely unreachable object - the cursor is
     * opened in its own frame so no local of this method keeps it on the stack - but it is a property of the runtime,
     * not a guarantee of the spec, so a different collector under load could in principle make it flake. It is asserted
     * rather than treated as best-effort on purpose: a test that silently passes when the safety net does not work
     * would be worse than one that occasionally needs a re-run.
     */
    @Test
    void aLeakedCursorStopsPinningTheFileOnceItIsCollected() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      leakACursorMidScan();

      assertThat(compacted.countActiveCursors()).as("the abandoned cursor is still reachable, so it still pins the file")
          .isPositive();

      // the reference the registry holds is weak, so collecting the cursor is enough to release the file
      awaitCollection(compacted);

      assertThat(compacted.countActiveCursors()).as("a collected-but-never-closed cursor must stop pinning the file").isZero();
    }

    /**
     * Opens a scan, advances it into the compacted series and drops every reference to it without closing. Kept in its
     * own frame so no local of the calling method keeps the cursor alive on the stack.
     */
    private void leakACursorMidScan() {
      final IndexCursor leaked = bucketIndex().iterator(true);
      assertThat(leaked.hasNext()).isTrue();
      leaked.next();
    }

    private void awaitCollection(final LSMTreeIndexCompacted compacted) {
      for (int attempt = 0; attempt < 50 && compacted.countActiveCursors() > 0; attempt++) {
        System.gc();
        try {
          Thread.sleep(20);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    /**
     * A {@link MultiIndexCursor} that fails to construct must not leave the children it was handed open - nothing else
     * holds a reference to them. It also NULLS their slots in the caller's list, because it keeps that list rather than
     * copying it, which is why {@code TypeIndex.range()} has to null-guard its own cleanup: without the guard it would
     * throw a {@link NullPointerException} over the exception being propagated.
     */
    @Test
    void aMultiIndexCursorThatFailsToConstructClosesTheChildrenAndClearsTheList() {
      final ProbeCountingCursor healthy = new ProbeCountingCursor(new RID(1, 1));
      final ProbeCountingCursor failing = new ProbeCountingCursor(new RID(1, 2));
      failing.failOnHasNext = true;
      final List<IndexCursor> children = new ArrayList<>(List.of(healthy, failing));

      assertThatThrownBy(() -> new MultiIndexCursor(children, -1, true))
          .as("the original failure must reach the caller, not a NullPointerException from the cleanup")
          .isInstanceOf(IllegalStateException.class).hasMessage("page read failed on purpose");

      assertThat(healthy.closeCalls).as("a child the caller can no longer reach must be closed").isOne();
      assertThat(failing.closeCalls).isOne();
      assertThat(children).as("the caller's own list is left holding nulls, hence the null guard in TypeIndex.range()")
          .containsOnlyNulls();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // item 4 - a cursor must iterate ITSELF
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aCollectionBackedCursorIteratesItself() {
      final List<Identifiable> rids = List.of(new RID(1, 1), new RID(1, 2), new RID(1, 3));
      final IndexCursorCollection cursor = new IndexCursorCollection(rids);

      assertThat(cursor.iterator()).as("a for-each must drive the cursor, not a second independent traversal").isSameAs(cursor);

      int seen = 0;
      for (final Identifiable rid : cursor) {
        // pre-#5662 the backing iterator was handed back directly, so next() - and with it getRecord() - was bypassed
        assertThat(cursor.getRecord()).as("getRecord() must track the element the for-each just produced").isEqualTo(rid);
        ++seen;
      }
      assertThat(seen).isEqualTo(rids.size());
    }

    /**
     * The vector search cursor was the one implementation that handed back the backing list's iterator, so a for-each
     * got a SECOND, independent traversal: it never advanced the cursor's own position, and mixing it with
     * {@code next()} read some RIDs twice.
     */
    @Test
    void theVectorSearchCursorIteratesItself() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE VectorDoc");
        database.command("sql", "CREATE PROPERTY VectorDoc.name STRING");
        database.command("sql", "CREATE PROPERTY VectorDoc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON VectorDoc (name) UNIQUE");
        database.command("sql", """
            CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
            METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }""");
      });
      database.transaction(() -> {
        database.newDocument("VectorDoc").set("name", "a").set("embedding", new float[] { 1f, 0f, 0f }).save();
        database.newDocument("VectorDoc").set("name", "b").set("embedding", new float[] { 0f, 1f, 0f }).save();
        database.newDocument("VectorDoc").set("name", "c").set("embedding", new float[] { 0f, 0f, 1f }).save();
      });

      final Index vectorIndex = database.getSchema().getType("VectorDoc").getPolymorphicIndexByProperties("embedding")
          .getIndexesOnBuckets()[0];

      try (final IndexCursor cursor = vectorIndex.get(new Object[] { new float[] { 1f, 0f, 0f } }, 3)) {
        assertThat(cursor.iterator()).as("a for-each must drive the cursor, not a second independent traversal")
            .isSameAs(cursor);

        int seen = 0;
        for (final Identifiable rid : cursor) {
          assertThat(cursor.getRecord()).as("getRecord() must track the element the for-each just produced").isEqualTo(rid);
          ++seen;
        }
        assertThat(seen).as("the fixture must return neighbours, otherwise the loop body never ran").isPositive();
      }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // item 5 - the MultiIndexCursor accessors must not probe the children
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void multiIndexCursorAccessorsDoNotDriveTheChildren() {
      final ProbeCountingCursor child = new ProbeCountingCursor(new RID(1, 1), new RID(1, 2));
      final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(child)), -1, true);

      final int probesAfterConstruction = child.hasNextCalls;
      assertThat(probesAfterConstruction).as("the fixture must have a child that CAN be probed").isPositive();

      cursor.getComparator();
      cursor.getBinaryKeyTypes();
      cursor.getComparator();
      cursor.getBinaryKeyTypes();

      // since #5635 hasNext() prefetches - it runs page reads and tombstone skips - so an accessor that probed the
      // children turned two plain getters into scan work
      assertThat(child.hasNextCalls).as("the accessors must answer from state sampled at construction")
          .isEqualTo(probesAfterConstruction);
    }

    @Test
    void multiIndexCursorStillReportsItsKeyTypesOnceExhausted() {
      final ProbeCountingCursor child = new ProbeCountingCursor(new RID(1, 1));
      final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(child)), -1, true);

      assertThat(cursor.getBinaryKeyTypes()).as("precondition: the key types are known while the cursor is live").isNotNull();

      while (cursor.hasNext())
        cursor.next();

      // the old formulation returned the first child that still HAD something, so an exhausted cursor reported null even
      // though its key types never changed
      assertThat(cursor.getBinaryKeyTypes()).as("key types do not depend on how much of the cursor is left").isNotNull();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // fixture
    // ---------------------------------------------------------------------------------------------------------------

    private LSMTreeIndexCompacted createCompactedFixture() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("value", Integer.class);
      // a small page size so the compacted output spans several pages, i.e. a real series a cursor walks lazily
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).withPageSize(1024).create();

      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE_NAME).set("value", i).save();
      });

      final LSMTreeIndex index = bucketIndex();
      try {
        if (index.scheduleCompaction())
          index.compact();
      } catch (final IOException | InterruptedException e) {
        throw new IllegalStateException("cannot compact the fixture index", e);
      }

      final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
      assertThat(compacted).as("the fixture must produce a compacted sub-index, otherwise no series cursor is ever opened")
          .isNotNull();
      assertThat(compacted.countActiveCursors()).as("no scan has run yet").isZero();
      return compacted;
    }

    private LSMTreeIndex bucketIndex() {
      return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];
    }

    /** Counts how many times an accessor drove it, which since #5635 is not a free operation on a real cursor. */
    private static final class ProbeCountingCursor implements IndexCursor {
      private final List<Identifiable> values;
      private       int                position;
      private       int                hasNextCalls;
      private       int                closeCalls;
      private       boolean            failOnHasNext;

      private ProbeCountingCursor(final Identifiable... values) {
        this.values = List.of(values);
      }

      @Override
      public Object[] getKeys() {
        return new Object[] { position };
      }

      @Override
      public Identifiable getRecord() {
        return position == 0 ? null : values.get(position - 1);
      }

      @Override
      public boolean hasNext() {
        ++hasNextCalls;
        if (failOnHasNext)
          // stands in for a lazy page read failing while the cursor is being wired up
          throw new IllegalStateException("page read failed on purpose");
        return position < values.size();
      }

      @Override
      public void close() {
        ++closeCalls;
      }

      @Override
      public Identifiable next() {
        if (position >= values.size())
          throw new NoSuchElementException();
        return values.get(position++);
      }

      @Override
      public long estimateSize() {
        return values.size() - position;
      }

      @Override
      public BinaryComparator getComparator() {
        return null;
      }

      @Override
      public byte[] getBinaryKeyTypes() {
        return new byte[] { 1 };
      }

      @Override
      public Iterator<Identifiable> iterator() {
        return this;
      }
    }
  }

  /**
   * #5681: a DML statement whose index scan stops short of exhaustion (a {@code LIMIT} cuts it off) used to leave the
   * scan's series cursor registered until the caller closed the returned {@link ResultSet} - or, if nobody ever did
   * (the overwhelmingly common shape for a {@code DELETE}/{@code UPDATE} run for its side effect, with the count read
   * or ignored), until the JVM collected the abandoned cursor.
   * <p>
   * {@code DeleteStatement.execute()} / {@code UpdateStatement.execute()} already drain the whole plan eagerly via
   * {@code executeInternal()} before ever handing a {@link ResultSet} back to the caller, so by the time the caller
   * gets it the statement is done and nothing will ever pull from the plan's steps again. These tests pin that the
   * plan's underlying index scan is released right there, without requiring the caller to close anything.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#5681 unclosed DML leaves the index scan open")
  class UnclosedDmlLeavesScanOpen extends TestHelper {
    private static final String TYPE_NAME = "Dml5681FollowUp";
    private static final int    RECORDS   = 600;

    @BeforeEach
    @Override
    public void beforeTest() {
      // explicit compaction only, so the sub-index appears exactly when this test asks for it
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    }

    @Test
    void deleteWithLimitReleasesTheIndexScanWithoutAnExplicitClose() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      database.transaction(() -> {
        // the returned ResultSet is deliberately never closed: this is the ordinary "DELETE used for its side effect"
        // shape from the issue, not a bug in the test
        final ResultSet result = database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE value > 1 LIMIT 3");
        assertThat(((Number) result.next().getProperty("count")).intValue()).as("the LIMIT must cut the scan short")
            .isEqualTo(3);
      });

      assertThat(compacted.countActiveCursors())
          .as("the DML statement is fully done once execute() returns - the abandoned index scan inside its sub-plan"
              + " must already be released, with no close() from the caller")
          .isZero();
    }

    @Test
    void updateWithLimitReleasesTheIndexScanWithoutAnExplicitClose() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      database.transaction(() -> {
        // same shape as the DELETE case: nobody closes the ResultSet returned by an UPDATE run for its side effect
        final ResultSet result = database.command("sql",
            "UPDATE " + TYPE_NAME + " SET touched = true WHERE value > 1 LIMIT 3");
        assertThat(((Number) result.next().getProperty("count")).intValue()).as("the LIMIT must cut the scan short")
            .isEqualTo(3);
      });

      assertThat(compacted.countActiveCursors())
          .as("the DML statement is fully done once execute() returns - the abandoned index scan inside its sub-plan"
              + " must already be released, with no close() from the caller")
          .isZero();
    }

    /**
     * {@code INSERT ... SELECT ... LIMIT} nests the same abandoned-scan shape one level deeper than DELETE/UPDATE -
     * the {@code LIMIT} lives inside the wrapped SELECT's own sub-plan rather than straddling the
     * {@code SubQueryStep} boundary - and {@code InsertExecutionPlan.fetchNext()} additionally re-invokes
     * {@code executeInternal()} on first pull (it does not know the eager call from {@code InsertStatement.execute()}
     * already ran), so this also pins that a second pull after the early {@code close()} does not resurrect the
     * closed index scan or throw.
     */
    @Test
    void insertSelectWithLimitReleasesTheIndexScanWithoutAnExplicitClose() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + "Copy"));

      database.transaction(() -> {
        // same shape as the DELETE/UPDATE cases: nobody closes the ResultSet returned by an INSERT run for its side effect
        final ResultSet result = database.command("sql",
            "INSERT INTO " + TYPE_NAME + "Copy SELECT FROM " + TYPE_NAME + " WHERE value > 1 LIMIT 3");
        assertThat(result.hasNext()).as("the INSERT must actually have produced rows").isTrue();
      });

      assertThat(compacted.countActiveCursors())
          .as("the DML statement is fully done once execute() returns - the abandoned index scan inside the nested"
              + " SELECT's sub-plan must already be released, with no close() from the caller")
          .isZero();

      final long copied = database.select().fromType(TYPE_NAME + "Copy").count();
      assertThat(copied).as("the LIMIT must actually cut the scan short, and a stray second pull must not double-insert")
          .isEqualTo(3);
    }

    /**
     * The {@code LIMIT}-less counterpart of the test above: without a {@code LIMIT}, the nested SELECT's own
     * {@code FetchFromIndexStep} drains to natural exhaustion - {@code fetchNextEntry()} drops its last cursor
     * reference on its own once there is nothing left to return - so the scan is already self-released before the
     * early {@code close()} added by this fix ever runs. Pinned separately from the {@code LIMIT} case (raised in
     * review) because it exercises a different release path: natural exhaustion instead of an abandoned-mid-scan
     * {@code close()}, and this is also the shape {@code InsertExecutionPlan}'s pre-existing reentrant second pull
     * (see {@link #insertSelectWithLimitReleasesTheIndexScanWithoutAnExplicitClose()}) most commonly hits, since a
     * plain {@code INSERT ... SELECT} with no {@code LIMIT} is the more common form.
     */
    @Test
    void insertSelectWithoutLimitReleasesTheIndexScanAndDoesNotDoubleInsert() {
      final LSMTreeIndexCompacted compacted = createCompactedFixture();

      database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + "Copy3"));

      database.transaction(() -> {
        // same shape as the other DML cases: nobody closes the ResultSet returned by an INSERT run for its side effect
        final ResultSet result = database.command("sql",
            "INSERT INTO " + TYPE_NAME + "Copy3 SELECT FROM " + TYPE_NAME + " WHERE value > 1");
        assertThat(result.hasNext()).as("the INSERT must actually have produced rows").isTrue();
      });

      assertThat(compacted.countActiveCursors())
          .as("a fully-drained scan releases itself on its own, and the fix's early close() must not disturb that")
          .isZero();

      final long copied = database.select().fromType(TYPE_NAME + "Copy3").count();
      assertThat(copied).as("all matching rows must be copied exactly once - the reentrant second executeInternal()"
          + " pull must not double-insert")
          .isEqualTo(RECORDS - 2);
    }

    private LSMTreeIndexCompacted createCompactedFixture() {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("value", Integer.class);
      // a small page size so the compacted output spans several pages, i.e. a real series a cursor walks lazily
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true).withPageSize(1024).create();

      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE_NAME).set("value", i).save();
      });

      final LSMTreeIndex index = bucketIndex();
      try {
        if (index.scheduleCompaction())
          index.compact();
      } catch (final IOException | InterruptedException e) {
        throw new IllegalStateException("cannot compact the fixture index", e);
      }

      final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
      assertThat(compacted).as("the fixture must produce a compacted sub-index, otherwise no series cursor is ever opened")
          .isNotNull();
      assertThat(compacted.countActiveCursors()).as("no scan has run yet").isZero();
      return compacted;
    }

    private LSMTreeIndex bucketIndex() {
      return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression tests for issue #5780: {@code isAutomatic()} answered the opposite of the truth on a manual index.
   * <p>
   * {@code LSMTreeIndex} and {@code HashIndex} both answered {@code metadata.propertyNames != null}, and
   * {@code IndexMetadata} coerces a null property list to an EMPTY one - so the field is never null and the predicate
   * was unconditionally true, including for the one index kind for which it must be false. "Automatic" here means
   * "there are indications of what to index", which is exactly what a manual index does not have.
   * <p>
   * Two consequences, both asserted below. {@code REBUILD INDEX *} and {@code COMPACT INDEX *} select their targets on
   * that predicate, so every manual index entered the sweep and failed inside the build with the vague "metadata
   * information are missing" - a database that merely CONTAINS a manual index made the bulk repair tool report a
   * failure for something that was never rebuildable by definition. And the accurate message both statements already
   * carry ("it's manual and there aren't indications of what to index") sat behind {@code if (!idx.isAutomatic())},
   * which could never be entered.
   */
  @Nested
  @DisplayName("#5780 manual index reported as automatic")
  class ManualIndexIsAutomatic extends TestHelper {
    private static final String TYPE_NAME   = "Doc";
    private static final String MANUAL_LSM  = "manualLsmIdx";
    private static final String MANUAL_HASH = "manualHashIdx";

    /**
     * Offset of the first key-type byte on a hash index metadata page, mirroring the package-private
     * {@code HashIndexBucket.META_KEY_TYPES_START} this package cannot see.
     */
    private static final int HASH_INDEX_META_KEY_TYPES_START = 9;

    private RID    RID_1;
    private RID    RID_2;
    private String typeIndexName;

    @Override
    public void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE_NAME).createProperty("name", Type.STRING);
        typeIndexName = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create().getName();

        RID_1 = database.newDocument(TYPE_NAME).set("name", "one").save().getIdentity();
        RID_2 = database.newDocument(TYPE_NAME).set("name", "two").save().getIdentity();
      });
    }

    /**
     * The predicate itself. A manual index of either kind that can be built without a type answers false, and a type
     * index - the wrapper and its bucket sub-index alike - keeps answering true.
     */
    @Test
    void aManualIndexIsNotAutomaticAndATypeIndexStillIs() {
      final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
      final Index manualHash = createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

      assertThat(manualLsm.isAutomatic())
          .as("an LSM_TREE manual index has no indications of what to index")
          .isFalse();
      assertThat(manualHash.isAutomatic())
          .as("a HASH manual index has no indications of what to index")
          .isFalse();

      final Index typeIndex = database.getSchema().getIndexByName(typeIndexName);
      assertThat(typeIndex.isAutomatic()).isTrue();
      for (final Index bucketIndex : ((TypeIndex) typeIndex).getIndexesOnBuckets())
        assertThat(bucketIndex.isAutomatic())
            .as("bucket sub-index '%s' is derived from the type's records", bucketIndex.getName())
            .isTrue();
    }

    /**
     * {@code REBUILD INDEX *} no longer targets what it cannot rebuild: the manual indexes are absent from the rebuilt
     * list AND - the assertion that discriminates the fix from the bug - absent from {@code failedIndexes}, which is
     * where they used to land. The automatic index is still rebuilt, so the sweep did not simply stop doing its job.
     */
    @Test
    void rebuildIndexAllSkipsManualIndexesInsteadOfFailingOnThem() {
      final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
      final Index manualHash = createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

      database.transaction(() -> {
        manualLsm.put(new Object[] { "a" }, new RID[] { RID_1 });
        manualHash.put(new Object[] { "b" }, new RID[] { RID_2 });
      });

      final Result row = database.command("sql", "rebuild index *").next();

      final List<String> rebuilt = row.getProperty("indexes");
      assertThat(rebuilt).doesNotContain(MANUAL_LSM, MANUAL_HASH);
      assertThat(rebuilt)
          .as("the sweep must still rebuild the automatic bucket sub-indexes")
          .isNotEmpty();

      assertThat((Object) row.getProperty("failedIndexes"))
          .as("a manual index is not a broken index: it must not be reported as a rebuild failure")
          .isNull();

      // The entries are still there. build() raised before emptying the index even before the fix, so this pins that
      // the fix did not introduce a destructive path of its own.
      database.transaction(() -> {
        assertThat(manualLsm.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
        assertThat(manualHash.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
      });
    }

    /**
     * A named rebuild of a manual index reports what is actually wrong with the request, from the guard written for
     * exactly this case, instead of the "metadata information are missing" raised two layers down inside the build.
     */
    @Test
    void rebuildIndexNamedOnAManualIndexReportsThatItIsManual() {
      createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

      assertThatThrownBy(() -> database.command("sql", "rebuild index `" + MANUAL_LSM + "`"))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(MANUAL_LSM)
          .hasMessageContaining("it's manual and there aren't indications of what to index");
    }

    /**
     * {@code COMPACT INDEX} selects its targets on the same predicate and carries the same guard, so the fix reaches
     * it too: the sweep skips a manual index and a named request is refused by the message that explains why.
     */
    @Test
    void compactIndexFollowsTheSamePredicate() {
      final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

      database.transaction(() -> manualLsm.put(new Object[] { "a" }, new RID[] { RID_1 }));

      final Result row = database.command("sql", "compact index *").next();
      assertThat((List<String>) row.getProperty("indexes")).doesNotContain(MANUAL_LSM);

      assertThatThrownBy(() -> database.command("sql", "compact index `" + MANUAL_LSM + "`"))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(MANUAL_LSM)
          .hasMessageContaining("it's manual and there aren't indications of what to index");

      // The sweep left the entries alone.
      database.transaction(() -> assertThat(manualLsm.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
    }

    /**
     * The other side of the predicate flip: a NAMED rebuild and a NAMED compaction of an automatic index still run.
     * <p>
     * The sweep test above already pins that the {@code *} form keeps rebuilding the bucket sub-indexes, but both
     * statements guard their single-index path on the same predicate, and that path takes an index the sweep never
     * hands it - the {@code TypeIndex} wrapper, which {@code *} deliberately excludes.
     */
    @Test
    void namedRebuildAndCompactStillRunOnAnAutomaticIndex() {
      // Present in the same database, so the flip is exercised with both kinds side by side.
      createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

      final Result rebuilt = database.command("sql", "rebuild index `" + typeIndexName + "`").next();
      assertThat((List<String>) rebuilt.getProperty("indexes")).contains(typeIndexName);
      assertThat((Object) rebuilt.getProperty("failedIndexes")).isNull();

      final String bucketIndexName = ((TypeIndex) database.getSchema().getIndexByName(typeIndexName))
          .getIndexesOnBuckets()[0].getName();
      final Result compacted = database.command("sql", "compact index `" + bucketIndexName + "`").next();
      assertThat((List<String>) compacted.getProperty("indexes")).contains(bucketIndexName);

      // The index still resolves its records after both operations.
      database.transaction(() -> assertThat(database.getSchema().getIndexByName(typeIndexName)
          .get(new Object[] { "one" }).next()).isEqualTo(RID_1));
    }

    /**
     * The predicate is public API and is reported verbatim by the schema views, so an operator listing the indexes now
     * reads the truth about which ones something populates.
     */
    @Test
    void theSchemaViewReportsAManualIndexAsNotAutomatic() {
      createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

      boolean seenManual = false;
      boolean seenAutomatic = false;
      final ResultSet rs = database.query("sql", "select from schema:indexes");
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String name = row.getProperty("name");
        if (MANUAL_LSM.equals(name)) {
          assertThat((Boolean) row.getProperty("automatic")).isFalse();
          seenManual = true;
        } else {
          assertThat((Boolean) row.getProperty("automatic"))
              .as("index '%s' is bound to a type, so it stays automatic", name)
              .isTrue();
          seenAutomatic = true;
        }
      }

      assertThat(seenManual).as("schema:indexes must list the manual index").isTrue();
      assertThat(seenAutomatic).as("schema:indexes must list the type indexes too").isTrue();
    }

    /**
     * A manual index whose name carries no underscore does not abort the schema load.
     * <p>
     * The orphan-relinking pass walks every TYPE-LESS index and splits its name on the last underscore to recover the
     * bucket a renamed sub-index belongs to - those are always {@code <bucketName>_<timestamp>}. A manual index is
     * type-less too but is named by the caller, so {@code lastIndexOf} answered -1 and the substring raised
     * {@code StringIndexOutOfBoundsException}, which {@code readConfiguration}'s own catch reported as "Error on
     * loading schema. The schema will be reset".
     * <p>
     * What that actually costs is everything the loader had not reached yet, since the pass sits in the middle of
     * {@code readConfiguration}: the bucket selection strategies, the triggers, the materialized views and continuous
     * aggregates, the function libraries, the extensions, and the compaction file-migration map WAL recovery redirects
     * through. The bucket selection strategy is the cheapest of those to pin, and it is the FIRST one after the pass,
     * so it is what this test asserts - the types themselves are parsed BEFORE the pass and survive either way, which
     * is why asserting their presence alone would not have caught this.
     */
    @Test
    void aManualIndexWithNoUnderscoreInItsNameDoesNotAbortTheSchemaLoad() {
      database.getSchema().getType(TYPE_NAME).setBucketSelectionStrategy("thread");

      createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
      createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

      reopenDatabase();

      assertThat(database.getSchema().getType(TYPE_NAME).getBucketSelectionStrategy().getName())
          .as("the load must reach the settings that follow the orphan-relinking pass")
          .isEqualTo("thread");

      assertThat(database.getSchema().existsType(TYPE_NAME)).isTrue();
      assertThat(database.getSchema().existsIndex(typeIndexName)).isTrue();
      assertThat(database.getSchema().existsIndex(MANUAL_LSM)).isTrue();
      assertThat(database.getSchema().existsIndex(MANUAL_HASH)).isTrue();
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(2L);

      assertThat(database.getSchema().getIndexByName(MANUAL_LSM).isAutomatic()).isFalse();
      assertThat(database.getSchema().getIndexByName(MANUAL_HASH).isAutomatic()).isFalse();
    }

    /**
     * {@code CHECK DATABASE FIX} reports a corrupt manual index and carries on, instead of aborting the whole repair on
     * it.
     * <p>
     * The rebuild loop resolves the index's associated bucket to recreate it there. A manual index has none, so
     * {@code getBucketById(-1)} raised "Bucket with id '-1' was not found" and the exception escaped the FIX before ANY
     * index - including the healthy type indexes of the same database - had been repaired. The index is also left
     * alone rather than dropped and recreated: its entries are not derived from any record, so the recreation this
     * loop performs would destroy them.
     */
    @Test
    void checkDatabaseFixReportsAManualIndexInsteadOfAbortingOnIt() {
      createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

      // The exact corruption HashIndexMetadataCorruptionTest injects: an invalid key-type byte on the metadata page.
      // It has to be re-read from disk to reach the cached key types, hence the reopen.
      corruptHashIndexKeyType(MANUAL_HASH);
      reopenDatabase();

      final Result row = database.command("sql", "check database fix").next();

      assertThat((Collection<String>) row.getProperty("corruptedIndexes"))
          .as("the finding must still reach the operator")
          .contains(MANUAL_HASH);
      assertThat((Collection<String>) row.getProperty("rebuiltIndexes"))
          .as("a manual index cannot be rebuilt from records, so FIX must not claim it rebuilt one")
          .doesNotContain(MANUAL_HASH);
      assertThat((Collection<String>) row.getProperty("warnings"))
          .anyMatch(w -> w.contains(MANUAL_HASH) && w.contains("manual index"));

      // The index is still there: FIX did not drop it, which on a manual index is an unrecoverable delete of its entries.
      assertThat(database.getSchema().existsIndex(MANUAL_HASH)).isTrue();
      // ...and the rest of the schema came through the repair pass untouched.
      assertThat(database.getSchema().existsIndex(typeIndexName)).isTrue();
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(2L);

      // Apply the repair the warning prescribes, so the class-wide post-test integrity check sees a clean database
      // rather than the corruption this test deliberately injected.
      database.getSchema().dropIndex(MANUAL_HASH);
    }

    /**
     * Overwrites the first key-type byte of a hash index metadata page (page 0) with a value no key type uses, which is
     * what {@code checkMetadataIntegrity()} reports as corruption.
     */
    private void corruptHashIndexKeyType(final String indexName) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = ((IndexInternal) db.getSchema().getIndexByName(indexName)).getFileIds().get(0);
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
          page.writeByte(HASH_INDEX_META_KEY_TYPES_START, (byte) -108);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    private Index createManualIndex(final String name, final Schema.INDEX_TYPE indexType, final boolean unique) {
      return database.getSchema().buildManualIndex(name, new Type[] { Type.STRING })
          .withType(indexType).withUnique(unique).create();
    }
  }
}
