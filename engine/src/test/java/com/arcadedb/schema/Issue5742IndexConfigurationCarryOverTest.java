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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.sparsevector.SegmentFormat;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5742: an index rebuilt from its own definition lost the configuration the WRAPPER index
 * types - full-text, geospatial, sparse-vector - keep outside the underlying LSM-Tree, because every carry-over site
 * read it with {@code IndexInternal.getMetadata()}, which on those types answers with the underlying index's plain
 * {@link IndexMetadata}. Four sites did it: {@code TRUNCATE TYPE}, {@code REBUILD INDEX} / {@code CHECK DATABASE FIX}
 * (for the sparse-vector type, where it did not merely lose the settings but failed outright), {@code ALTER TYPE ...
 * BUCKET +...} and {@code ALTER TYPE ... SUPERTYPE +...}. Each is a distinct user-visible operation, so each gets its
 * own case here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5742IndexConfigurationCarryOverTest extends TestHelper {

  private static final String ENGLISH_ANALYZER = "org.apache.lucene.analysis.en.EnglishAnalyzer";

  @Test
  void truncateTypeKeepsTheFullTextConfiguration() {
    createFullTextFixture("Doc");
    database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

    database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

    assertThat(fullTextMetadataOf("Doc").getAnalyzerClass()).isEqualTo(ENGLISH_ANALYZER);
    assertThat(fullTextMetadataOf("Doc").getBm25K1()).isEqualTo(1.5f);

    database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());
    assertThat(indexOf("Doc").get(new Object[] { "fox" }).hasNext())
        .as("the English stem must still match after the truncate").isTrue();
  }

  @Test
  void truncateTypeKeepsTheGeospatialPrecision() {
    createGeoFixture("Doc", 7);
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'Rome', coords = 'POINT (12.5 41.9)'").close());

    database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

    assertThat(((LSMTreeGeoIndex) subIndexOf("Doc")).getPrecision()).isEqualTo(7);
  }

  @Test
  void truncateTypeKeepsTheVectorConfiguration() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}").close();
    });
    database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

    database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

    final LSMVectorIndexMetadata metadata = (LSMVectorIndexMetadata) indexOf("Doc").getMetadataForNewFile();
    assertThat(metadata.dimensions).isEqualTo(4);
  }

  @Test
  void addingABucketKeepsTheFullTextConfiguration() {
    createFullTextFixture("Doc");

    database.command("sql", "ALTER TYPE Doc BUCKET +Doc_extra").close();

    for (final IndexInternal sub : ((TypeIndex) indexOf("Doc")).getSubIndexes())
      assertThat(((FullTextIndexMetadata) sub.getMetadataForNewFile()).getAnalyzerClass())
          .as("every bucket sub-index must carry the analyzer").isEqualTo(ENGLISH_ANALYZER);
  }

  @Test
  void addingASuperTypeKeepsTheFullTextConfiguration() {
    createFullTextFixture("Super");
    database.transaction(() -> database.getSchema().createDocumentType("Sub").createProperty("text", Type.STRING));

    database.command("sql", "ALTER TYPE Sub SUPERTYPE +Super").close();

    final TypeIndex index = (TypeIndex) indexOf("Super");
    assertThat(index.getSubIndexes().size()).as("the index must have been propagated onto Sub's buckets")
        .isGreaterThan(1);
    for (final IndexInternal sub : index.getSubIndexes())
      assertThat(((FullTextIndexMetadata) sub.getMetadataForNewFile()).getAnalyzerClass())
          .as("every propagated sub-index must carry the analyzer").isEqualTo(ENGLISH_ANALYZER);
  }

  @Test
  void rebuildIndexKeepsTheSparseVectorConfiguration() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Doc");
      type.createProperty("dims", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (dims, weights) LSM_SPARSE_VECTOR"
          + " METADATA {dimensions: 128, modifier: 'IDF', weightQuantization: 'FP16'}").close();
    });

    database.command("sql", "REBUILD INDEX `Doc[dims,weights]`").close();

    final LSMSparseVectorIndexMetadata metadata = (LSMSparseVectorIndexMetadata) indexOf("Doc").getMetadataForNewFile();
    assertThat(metadata.dimensions).isEqualTo(128);
    assertThat(metadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
    assertThat(metadata.weightQuantization).isEqualTo(SegmentFormat.WeightQuantization.FP16);
  }

  /**
   * The sparse-vector wrapper keeps its dimensionality, scoring modifier and weight quantization in a metadata of its
   * own, and its builder REFUSES a plain {@link IndexMetadata}: the shared rebuild path did not merely lose those
   * settings, it could not rebuild the index at all ("An LSM_SPARSE_VECTOR index requires
   * LSMSparseVectorIndexMetadata but got com.arcadedb.schema.IndexMetadata").
   */
  @Test
  void checkDatabaseFixKeepsTheSparseVectorConfiguration() {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc").close();
      database.command("sql", "CREATE PROPERTY Doc.dims ARRAY_OF_INTEGERS").close();
      database.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS").close();
      final Result inserted = database.command("sql", "INSERT INTO Doc SET dims = [1,5], weights = [0.5,0.25]").next();
      database.command("sql", "CREATE INDEX sparseDoc ON Doc (dims, weights) LSM_SPARSE_VECTOR"
          + " METADATA {dimensions: 128, modifier: 'IDF', weightQuantization: 'FP16'}").close();
      victim.set(inserted.toElement().getIdentity());
    });

    corruptRecordTypeByte((DatabaseInternal) database, victim.get());

    try (final ResultSet result = database.command("sql", "CHECK DATABASE FIX")) {
      assertThat(result.next().<Long>getProperty("autoFix")).isGreaterThan(0L);
    }

    assertThat(database.getSchema().existsIndex("sparseDoc")).as("the repaired index must keep its manual name").isTrue();
    final IndexMetadata repaired = ((IndexInternal) database.getSchema().getIndexByName("sparseDoc")).getMetadataForNewFile();
    assertThat(repaired).as("the repaired index must still be a sparse-vector index, not a plain one")
        .isInstanceOf(LSMSparseVectorIndexMetadata.class);
    final LSMSparseVectorIndexMetadata metadata = (LSMSparseVectorIndexMetadata) repaired;
    assertThat(metadata.dimensions).isEqualTo(128);
    assertThat(metadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
    assertThat(metadata.weightQuantization).isEqualTo(SegmentFormat.WeightQuantization.FP16);
  }

  /**
   * The two halves of a wrapper index's definition are joined again on schema RELOAD, and the bucket-level index JSON
   * the type-specific half is read from carries no {@code typeName} key - so the base-field read is skipped and the
   * collations and the user-supplied index name are only on the underlying definition. A carry-over site that reads
   * the type-specific half would then trade one silent loss for another: right analyzer, lost index name.
   */
  @Test
  void truncateAfterAReopenKeepsBothHalvesOfTheDefinition() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("text", Type.STRING);
      database.command("sql", "CREATE INDEX myFullText ON Doc (text) FULL_TEXT"
          + " METADATA {\"analyzer\": \"" + ENGLISH_ANALYZER + "\"}").close();
    });
    database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

    reopenDatabase();

    database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

    assertThat(database.getSchema().existsIndex("myFullText")).as("the truncated index must keep its manual name").isTrue();
    assertThat(((FullTextIndexMetadata) ((IndexInternal) database.getSchema().getIndexByName("myFullText"))
        .getMetadataForNewFile()).getAnalyzerClass())
        .as("and its analyzer").isEqualTo(ENGLISH_ANALYZER);
  }

  private void createFullTextFixture(final String typeName) {
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName).createProperty("text", Type.STRING);
      final TypeFullTextIndexBuilder builder = (TypeFullTextIndexBuilder) database.getSchema()//
          .buildTypeIndex(typeName, new String[] { "text" }).withType(Schema.INDEX_TYPE.FULL_TEXT);
      builder.withAnalyzer(ENGLISH_ANALYZER).withBM25(1.5f, 0.5f);
      builder.create();
    });
  }

  private void createGeoFixture(final String typeName, final int precision) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("name", Type.STRING);
      type.createProperty("coords", Type.STRING);
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(typeName, new String[] { "coords" })//
          .withType(Schema.INDEX_TYPE.GEOSPATIAL);
      final GeoIndexMetadata metadata = new GeoIndexMetadata(typeName, new String[] { "coords" }, -1);
      metadata.setPrecision(precision);
      builder.withMetadata(metadata);
      builder.create();
    });
  }

  private IndexInternal indexOf(final String typeName) {
    return (IndexInternal) database.getSchema().getType(typeName).getAllIndexes(false).iterator().next();
  }

  private IndexInternal subIndexOf(final String typeName) {
    return ((TypeIndex) indexOf(typeName)).getSubIndexes().get(0);
  }

  private FullTextIndexMetadata fullTextMetadataOf(final String typeName) {
    return (FullTextIndexMetadata) ((LSMTreeFullTextIndex) subIndexOf(typeName)).getMetadataForNewFile();
  }
}
