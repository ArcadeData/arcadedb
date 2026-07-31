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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeLSMSparseVectorIndexBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5607: Studio used to emit {@code CREATE INDEX ... LSM_VECTOR} without a
 * METADATA clause, which the engine rejects. The Studio side is covered by
 * {@code studio/test/create-index-command.test.js}; this test pins the server-side contract the
 * dialog has to satisfy:
 * <ul>
 *   <li>a missing METADATA clause is refused with a message that names the mandatory setting;</li>
 *   <li>a METADATA clause that omits (or zeroes) {@code dimensions} is refused as well, instead of
 *       silently creating an index that can never match a vector, since every put() compares
 *       {@code vector.length} against {@code metadata.dimensions};</li>
 *   <li>the statement Studio now emits is accepted and carries the settings through.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5607VectorIndexMetadataTest extends TestHelper {

  @Test
  void denseVectorIndexWithoutMetadataIsRefusedNamingDimensions() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    // A parsing exception, not an execution one: the HTTP layer maps it to 400 rather than 500.
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimensions");

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
  }

  @Test
  void denseVectorIndexWithoutDimensionsIsRefused() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    // An empty METADATA clause satisfied the old "metadata != null" check and produced an index with
    // dimensions=0, which never indexes anything.
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimensions");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"similarity\": \"COSINE\"}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimensions");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 0}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimensions");

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();
  }

  /**
   * A METADATA value the builder cannot read comes from the statement, so it is a client mistake. Each
   * of these used to escape as a raw {@code NumberFormatException} or {@code IndexException}, which the
   * HTTP layer reports as a 500; they are parsing errors now, so the answer stays a 400.
   */
  @Test
  void unreadableDenseVectorMetadataValuesAreParsingErrors() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": \"abc\"}"))
        .isInstanceOf(CommandSQLParsingException.class);

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"similarity\": \"NOPE\"}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("similarity");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"quantization\": \"NOPE\"}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("quantization");

    // pqClusters is capped at 256 because a PQ code is one byte per subspace (issue #5417).
    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 8, \"pqClusters\": 512}"))
        .isInstanceOf(CommandSQLParsingException.class);

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();

    // A number that merely arrived quoted is still a number: it must keep working.
    assertThatCode(
        () -> database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": \"8\"}"))
        .doesNotThrowAnyException();
    assertThat(((LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
        .getIndexesOnBuckets()[0]).getDimensions()).isEqualTo(8);
  }

  /**
   * Every quantization the dialog offers has to survive the shape the dialog produces: an index created
   * on a type that is still empty, filled afterwards. PRODUCT is the hard case (its codebooks are trained
   * from the data, and there is none at creation time), but an option that only fails on the first insert
   * would be no better than the bug this issue is about, so each one is exercised end to end.
   */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8", "BINARY", "PRODUCT" })
  void everyQuantizationOfferedByTheDialogIndexesAndSearchesVectors(final String quantization) {
    final String typeName = "QDoc" + quantization;

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE " + typeName);
      database.command("sql", "CREATE PROPERTY " + typeName + ".embedding ARRAY_OF_FLOATS");
    });

    database.command("sql", "CREATE INDEX ON `" + typeName + "` (`embedding`) LSM_VECTOR METADATA "
        + "{\"dimensions\":8,\"similarity\":\"COSINE\",\"maxConnections\":32,\"beamWidth\":100,\"quantization\":\""
        + quantization + "\"}");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = database.newVertex(typeName);
        final float[] vector = new float[8];
        for (int j = 0; j < 8; j++)
          vector[j] = (i + j) / 64f;
        v.set("embedding", vector);
        v.save();
      }
    });

    final ResultSet rs = database.query("sql",
        "SELECT distance FROM (SELECT expand(vectorNeighbors('" + typeName + "[embedding]', ?, 5)))",
        (Object) new float[] { 0f, 1 / 64f, 2 / 64f, 3 / 64f, 4 / 64f, 5 / 64f, 6 / 64f, 7 / 64f });
    assertThat(rs.stream().count()).as("quantization=%s must answer a search", quantization).isPositive();
  }

  @Test
  void denseVectorIndexFromTheStudioDialogIsAccepted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    // Verbatim shape of the statement buildCreateIndexCommand() emits for the LSM_VECTOR branch.
    database.command("sql", "CREATE INDEX ON `Doc` (`embedding`) LSM_VECTOR METADATA "
        + "{\"dimensions\":8,\"similarity\":\"DOT_PRODUCT\",\"maxConnections\":24,\"beamWidth\":120}");

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    assertThat(typeIndex).isNotNull();

    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(index.getDimensions()).isEqualTo(8);
    assertThat(index.getSimilarityFunction().name()).isEqualTo("DOT_PRODUCT");
    assertThat(index.getMetadata().maxConnections).isEqualTo(24);
    assertThat(index.getMetadata().beamWidth).isEqualTo(120);
  }

  @Test
  void denseVectorIndexFromTheJavaApiWithoutDimensionsIsRefused() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withType(Schema.INDEX_TYPE.LSM_VECTOR)
        .withLSMVectorType()
        .withSimilarity("COSINE")
        .create())
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("dimensions");
  }

  /**
   * The sparse branch of the dialog was the only one that ever set a METADATA clause. Everything
   * LSM_SPARSE_VECTOR reads from metadata is optional, so both the bare and the fully populated form
   * the dialog can emit must be accepted (issue #5607 asked for this to be verified explicitly).
   */
  @Test
  void sparseVectorIndexAcceptsBothTheBareAndThePopulatedStudioStatement() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Sparse");
      database.command("sql", "CREATE PROPERTY Sparse.dims ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE VERTEX TYPE Sparse2");
      database.command("sql", "CREATE PROPERTY Sparse2.dims ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse2.weights ARRAY_OF_FLOATS");
    });

    // No METADATA at all: every sparse setting has a default.
    assertThatCode(() -> database.command("sql", "CREATE INDEX ON `Sparse` (`dims`, `weights`) LSM_SPARSE_VECTOR"))
        .doesNotThrowAnyException();

    assertThatCode(() -> database.command("sql", "CREATE INDEX ON `Sparse2` (`dims`, `weights`) LSM_SPARSE_VECTOR METADATA "
        + "{\"dimensions\":105000,\"modifier\":\"IDF\",\"weightQuantization\":\"FP16\"}"))
        .doesNotThrowAnyException();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Sparse2[dims,weights]");
    assertThat(typeIndex).isNotNull();
    assertThat(typeIndex.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);
  }

  /**
   * Guards the builder alias used above: the sparse builder must keep accepting the three keys the
   * dialog can emit without needing a dedicated setter call per knob.
   */
  @Test
  void sparseVectorBuilderIsReachableFromTheGenericTypeIndexBuilder() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Sparse");
      database.command("sql", "CREATE PROPERTY Sparse.dims ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
    });

    final TypeLSMSparseVectorIndexBuilder builder = database.getSchema()
        .buildTypeIndex("Sparse", new String[] { "dims", "weights" })
        .withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
        .withSparseVectorType();

    assertThatCode(builder::create).doesNotThrowAnyException();
  }
}
