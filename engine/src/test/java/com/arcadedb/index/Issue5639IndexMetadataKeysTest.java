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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorEncoding;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue5639IndexMetadataKeysTest extends TestHelper {

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
          "pqCenterGlobally": false
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

    final String bucketName = database.getSchema().getType("Doc").getBuckets(false).getFirst().getName();
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

    final String bucketName = database.getSchema().getType("Doc").getBuckets(false).getFirst().getName();
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
        .withPQTrainingLimit(4096);

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
