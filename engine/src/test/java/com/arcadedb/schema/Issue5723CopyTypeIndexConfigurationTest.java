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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.ResultSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5723: {@code LocalSchema.copyType()} recreated the source type's indexes through the
 * three-argument {@code createTypeIndex} overload, so every attribute of the definition other than the index type, the
 * uniqueness flag and the property list was silently replaced by a default on the copy - the tuned page size, the null
 * strategy, the collations, and the whole type-specific configuration of a full-text, geospatial or vector index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5723CopyTypeIndexConfigurationTest extends TestHelper {

  private static final String ENGLISH_ANALYZER = "org.apache.lucene.analysis.en.EnglishAnalyzer";

  @Test
  void copyTypeKeepsThePageSize() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(16_384).create();
    });
    database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

    assertThat(indexOf("Doc").getPageSize()).as("the fixture must not use the default page size").isEqualTo(16_384);

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    final IndexInternal copied = indexOf("Doc2");
    assertThat(copied.getPageSize()).as("the copy must keep the page size of the original").isEqualTo(16_384);
    assertThat(copied.isUnique()).isTrue();
    assertThat(copied.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(copied.countEntries()).as("the copy must be populated with the copied records").isEqualTo(1);
  }

  /**
   * A HASH index cannot be created above 65536 bytes (#5713), and the page size a copy asks for goes back through that
   * same validating creation path. Carrying the current value blindly would make copying a type that predates the guard
   * fail; {@code getPageSizeForNewFile()} answers with a legal one instead.
   */
  @Test
  void copyTypeKeepsTheHashPageSize() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).withPageSize(8_192).create();
    });
    database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

    assertThat(indexOf("Doc").getPageSize()).isEqualTo(8_192);

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    assertThat(indexOf("Doc2").getPageSize()).as("the copy must keep the page size of the original").isEqualTo(8_192);
    assertThat(indexOf("Doc2").getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
  }

  @Test
  void copyTypeKeepsTheNullStrategy() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)//
          .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR).create();
    });

    assertThat(indexOf("Doc").getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    assertThat(indexOf("Doc2").getNullStrategy()).as("the copy must keep the null strategy of the original")//
        .isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
  }

  /**
   * The collation is what makes a lookup case-insensitive, so losing it turns a working query into one that returns
   * nothing. Asserted on behaviour, not only on the persisted attribute.
   */
  @Test
  void copyTypeKeepsTheCaseInsensitiveCollation() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.command("sql", "CREATE INDEX ON Doc (k COLLATE CI) UNIQUE");
    });
    database.transaction(() -> database.newDocument("Doc").set("k", "Hello").save());

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    assertThat(indexOf("Doc2").getMetadata().collations).as("the copy must keep the collations of the original")//
        .isEqualTo(List.of(IndexMetadata.COLLATION_CI));

    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc2 WHERE k = 'HELLO'")) {
      assertThat(rs.hasNext()).as("the case-insensitive lookup must still match on the copy").isTrue();
    }
  }

  /**
   * The analyzer and the BM25 parameters live on the full-text wrapper, not on the LSM-Tree underneath it, so the
   * carry-over has to read them from there. A copy that lost them tokenizes differently and ranks differently.
   */
  @Test
  void copyTypeKeepsTheFullTextConfiguration() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("text", Type.STRING);
      final TypeFullTextIndexBuilder builder = (TypeFullTextIndexBuilder) database.getSchema()//
          .buildTypeIndex("Doc", new String[] { "text" }).withType(Schema.INDEX_TYPE.FULL_TEXT);
      builder.withAnalyzer(ENGLISH_ANALYZER).withAllowLeadingWildcard(true).withDefaultOperator("AND")//
          .withBM25(1.5f, 0.5f);
      builder.create();
    });
    database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    final FullTextIndexMetadata copied = fullTextMetadataOf("Doc2");
    assertThat(copied.getAnalyzerClass()).as("the copy must keep the analyzer of the original").isEqualTo(ENGLISH_ANALYZER);
    assertThat(copied.isAllowLeadingWildcard()).isTrue();
    assertThat(copied.getDefaultOperator()).isEqualTo("AND");
    assertThat(copied.getBm25K1()).isEqualTo(1.5f);
    assertThat(copied.getBm25B()).isEqualTo(0.5f);
    assertThat(copied.getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
    // The English analyzer stems "foxes" to "fox": the record is findable through the stem only if the analyzer survived.
    assertThat(indexOf("Doc2").get(new Object[] { "fox" }).hasNext()).isTrue();
  }

  /**
   * The geohash resolution and the storage layout are plain fields on the geospatial wrapper. Losing the resolution
   * changes the cell size the index is built with, silently.
   */
  @Test
  void copyTypeKeepsTheGeospatialPrecision() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Doc");
      type.createProperty("name", Type.STRING);
      type.createProperty("coords", Type.STRING);
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "coords" })//
          .withType(Schema.INDEX_TYPE.GEOSPATIAL);
      final GeoIndexMetadata metadata = new GeoIndexMetadata("Doc", new String[] { "coords" }, -1);
      metadata.setPrecision(7);
      builder.withMetadata(metadata);
      builder.create();
    });
    database.transaction(//
        () -> database.command("sql", "INSERT INTO Doc SET name = 'Rome', coords = 'POINT (12.5 41.9)'"));

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    final LSMTreeGeoIndex copied = (LSMTreeGeoIndex) subIndexOf("Doc2");
    assertThat(copied.getPrecision()).as("the copy must keep the geohash precision of the original").isEqualTo(7);
    assertThat(copied.getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FRONTIER);

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Doc2 WHERE geo.intersects(coords,"
        + " geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /**
   * Without its dimensions and similarity a vector index is not merely differently tuned, it is unusable: the copy
   * would come up with dimensions 0 and reject every write.
   */
  @Test
  void copyTypeKeepsTheVectorConfiguration() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}");
    });
    database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    final LSMVectorIndexMetadata copied = (LSMVectorIndexMetadata) indexOf("Doc2").getMetadataForNewFile();
    assertThat(copied.dimensions).as("the copy must keep the dimensions of the original").isEqualTo(4);
    assertThat(copied.similarityFunction).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
  }

  /**
   * A user-supplied index name is unique across the schema and still belongs to the source type, so the copy takes the
   * auto-derived form rather than colliding with it.
   */
  @Test
  void copyTypeDoesNotReuseTheManualIndexName() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.command("sql", "CREATE INDEX myIndex ON Doc (k) UNIQUE");
    });

    database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

    assertThat(database.getSchema().existsIndex("myIndex")).isTrue();
    assertThat(database.getSchema().getIndexByName("myIndex").getTypeName()).isEqualTo("Doc");
    assertThat(database.getSchema().getType("Doc2").getAllIndexes(false).iterator().next().getName()).isEqualTo("Doc2[k]");
  }

  /**
   * The conversion that motivates {@code copyType()} in the first place is document to vertex, and that goes through
   * the other branch of the record-copy loop ({@code database.newVertex(...)}). Every other case here copies to a
   * document type, so this one covers the branch the reported scenario actually takes.
   */
  @Test
  void copyTypeToAVertexTypeKeepsTheDefinitionAndTheRecords() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
      database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(16_384).create();
    });
    database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

    database.getSchema().copyType("Doc", "Doc2", LocalVertexType.class, 1, 65_536, 1_000);

    assertThat(database.getSchema().getType("Doc2")).isInstanceOf(LocalVertexType.class);

    final IndexInternal copied = indexOf("Doc2");
    assertThat(copied.getPageSize()).isEqualTo(16_384);
    assertThat(copied.countEntries()).as("the vertex copy must be populated with the copied records").isEqualTo(1);

    try (final ResultSet rs = database.query("sql", "SELECT k FROM Doc2 WHERE k = 'v1'")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("k")).as("the vertex copy must carry the record content").isEqualTo("v1");
    }
  }

  /**
   * {@code LSMVectorIndexMetadata.copy()} carried only the collations before this change and now goes through
   * {@code copyCommonTo()}, which also carries the user-supplied index name. That reaches one caller outside
   * {@code copyType()} - {@code TRUNCATE TYPE}, which drops and recreates the index from its own definition - so a
   * manually named vector index now keeps its name across a truncate instead of reverting to the auto-derived form.
   * Asserted here rather than left to be inferred.
   */
  @Test
  void truncatingAManuallyNamedVectorIndexKeepsItsName() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX myVectorIndex ON Doc (embedding) LSM_VECTOR"//
          + " METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}");
    });
    database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

    assertThat(database.getSchema().existsIndex("myVectorIndex")).isTrue();

    database.transaction(() -> database.command("sql", "TRUNCATE TYPE Doc"));

    assertThat(database.getSchema().existsIndex("myVectorIndex")).as("the truncated index must keep its manual name")//
        .isTrue();
    final LSMVectorIndexMetadata metadata = (LSMVectorIndexMetadata) //
        ((IndexInternal) database.getSchema().getIndexByName("myVectorIndex")).getMetadataForNewFile();
    assertThat(metadata.dimensions).isEqualTo(4);
    assertThat(metadata.similarityFunction).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
  }

  private IndexInternal indexOf(final String typeName) {
    return (IndexInternal) database.getSchema().getType(typeName).getAllIndexes(false).iterator().next();
  }

  private IndexInternal subIndexOf(final String typeName) {
    return ((TypeIndex) indexOf(typeName)).getSubIndexes().getFirst();
  }

  private FullTextIndexMetadata fullTextMetadataOf(final String typeName) {
    return (FullTextIndexMetadata) ((LSMTreeFullTextIndex) subIndexOf(typeName)).getMetadataForNewFile();
  }
}
