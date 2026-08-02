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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5765, item 3: {@code CREATE INDEX IF NOT EXISTS} compared the STRUCTURAL definition
 * only - the index kind and its uniqueness - so an existing index configured differently satisfied a request that
 * spelled out other settings. A guarded statement asking for 768 dimensions over a 384-dimension index was a silent
 * no-op, which is the shape of surprise issue #5675 removed one level up.
 * <p>
 * The rule these tests pin down: only the settings the {@code METADATA} clause NAMED are compared. A statement that
 * names none compares none and stays the plain no-op it has always been; a statement that names one gets an answer
 * about it, whether or not a rebuild would have been needed to change it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5765IndexSettingsIfNotExistsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.getSchema().getType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.getSchema().getType("Doc").createProperty("title", Type.STRING);
      database.getSchema().getType("Doc").createProperty("location", Type.STRING);
    });
  }

  /**
   * The reported case: the existing vector index has 384 dimensions and the guarded statement asks for 768. Every
   * vector the caller then writes is 768 long and none of them is indexed, which is exactly what the guard was
   * supposed to prevent.
   */
  @Test
  void guardedVectorDimensionsMismatchIsReported() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 768}"))
        .hasMessageContaining("dimensions=384")
        .hasMessageContaining("768");

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
  }

  /**
   * Same setting, same value: the statement stays a no-op and the index is not touched.
   */
  @Test
  void guardedMatchingSettingsStayANoOp() {
    createVectorIndex(384, "COSINE");

    final ResultSet rs = database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 384, \"similarity\": \"COSINE\"}");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
  }

  /**
   * The clause is read through the index type's own reader before comparing, so a quoted number and a lower-case enum
   * name are the value they denote, not a difference.
   */
  @Test
  void equivalentSpellingsAreNotAMismatch() {
    createVectorIndex(384, "EUCLIDEAN");

    final ResultSet rs = database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": \"384\", \"similarity\": \"euclidean\"}");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
  }

  /**
   * A setting other than the dimensions is compared the same way - here the similarity function, which decides what
   * "nearest" means and so silently changes every result the caller gets.
   */
  @Test
  void guardedVectorSimilarityMismatchIsReported() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 384, \"similarity\": \"EUCLIDEAN\"}"))
        .hasMessageContaining("similarity=COSINE");

    assertThat(similarityOf("Doc[embedding]")).isEqualTo("COSINE");
  }

  /**
   * Full-text analyzers decide how the text is tokenized, so an index built with another one answers different
   * queries. Reported, and the existing index survives.
   */
  @Test
  void guardedFullTextAnalyzerMismatchIsReported() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"}"))
        .hasMessageContaining("analyzer=org.apache.lucene.analysis.core.KeywordAnalyzer");

    assertThat(database.getSchema().existsIndex("Doc[title]")).isTrue();
  }

  /**
   * Geospatial precision decides the cell resolution, so it changes what a query has to post-filter.
   */
  @Test
  void guardedGeospatialPrecisionMismatchIsReported() {
    database.command("sql", "CREATE INDEX ON Doc (location) GEOSPATIAL METADATA {\"precision\": 8}");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (location) GEOSPATIAL METADATA {\"precision\": 11}"))
        .hasMessageContaining("precision=8");

    assertThat(database.getSchema().existsIndex("Doc[location]")).isTrue();
  }

  /**
   * The no-behavior-change case, and the reason the comparison is limited to the named keys: a guarded statement with
   * no {@code METADATA} names nothing, so it stays the plain no-op it has always been even over an index configured
   * with settings it says nothing about.
   */
  @Test
  void aGuardedStatementWithoutMetadataStaysANoOp() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
  }

  /**
   * The settings are compared only once the structural definition matches: two index KINDS have unrelated key spaces,
   * so the conflict to report is the kind, not a list of settings one of them does not have.
   */
  @Test
  void aKindMismatchIsStillReportedAsAKindMismatch() {
    database.command("sql", "CREATE INDEX ON Doc (title) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LSM_TREE")
        .hasMessageContaining("FULL_TEXT")
        .hasMessageNotContaining("analyzer=");
  }

  private void createVectorIndex(final int dimensions, final String similarity) {
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + dimensions
        + ", \"similarity\": \"" + similarity + "\"}");
  }

  private int dimensionsOf(final String indexName) {
    return vectorMetadataOf(indexName).dimensions;
  }

  private String similarityOf(final String indexName) {
    return vectorMetadataOf(indexName).similarityFunction.name();
  }

  private LSMVectorIndexMetadata vectorMetadataOf(final String indexName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(typeIndex.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);
    return (LSMVectorIndexMetadata) typeIndex.getMetadataForNewFile();
  }
}
