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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketLSMVectorIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMSparseVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vector index metadata, creation and schema reload. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorIndexSchemaTest {
  /**
   * Regression for #5607: vector index metadata.
   * <p>
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
  @Nested
  class VectorIndexMetadata extends TestHelper {

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

  /**
   * Regression for #11568: persisted vector metadata validation.
   * <p>
   * Issue #11568: {@code CREATE INDEX ... LSM_VECTOR} refuses {@code pqClusters > 256}, but a {@code schema.json} edited to carry
   * 512 was loaded as is, and the runtime index held a value no creation path can produce. The persisted definition is now read
   * through the setters that enforce the creation invariants.
   * <p>
   * A refused definition must cost its own index only. A value the load already validated (an unknown similarity, a negative
   * {@code rerankOversample}) escaped into the catch of the whole schema load, which reported the schema as "reset" and left out
   * every index of every type the loop had not reached: an unrelated UNIQUE index then enforced nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PersistedVectorMetadataValidation extends TestHelper {
    private static final String VECTOR_INDEX = "Vec[vector]";
    // sorts after "Vec", so the index loop reaches it after the vector index: what the schema reset used to lose
    private static final String UNIQUE_INDEX = "Zzz[name]";

    @Test
    void aPersistedPqClustersAboveTheCreationLimitIsRefusedByTheMetadata() {
      final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Vec", new String[] { "vector" }, 0);
      final JSONObject persisted = new JSONObject().put("typeName", "Vec").put("properties", new JSONArray().put("vector"))
          .put("dimensions", 4).put("quantization", "PRODUCT").put("pqClusters", 512);
      assertThatThrownBy(() -> metadata.fromJSON(persisted)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("pqClusters cannot exceed 256");

      // the creation path refuses the same value with the same message
      database.command("sql", "CREATE DOCUMENT TYPE BadVec");
      database.command("sql", "CREATE PROPERTY BadVec.vector ARRAY_OF_FLOATS");
      assertThatThrownBy(() -> database.command("sql",
          "CREATE INDEX ON BadVec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', quantization: 'PRODUCT', pqClusters: 512}"))
          .hasStackTraceContaining("pqClusters cannot exceed 256");
    }

    @ParameterizedTest(name = "pqClusters {0}")
    @CsvSource({ "4294967297", "256.5" })
    void aPersistedIntegerIsNotNarrowedPastItsValidation(final String value) {
      // getInt() narrowed first: 4294967297 reached the setter as 1 and 256.5 as 256, both of which it accepts
      final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Vec", new String[] { "vector" }, 0);
      final JSONObject persisted = new JSONObject("{\"typeName\":\"Vec\",\"properties\":[\"vector\"],\"pqClusters\":" + value + "}");
      assertThatThrownBy(() -> metadata.fromJSON(persisted)).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("pqClusters");
    }

    @Test
    void theAutoPqSubspacesOfAPersistedDefinitionIsStillAccepted() {
      final LSMVectorIndexMetadata metadata = new LSMVectorIndexMetadata("Vec", new String[] { "vector" }, 0);
      metadata.fromJSON(new JSONObject().put("typeName", "Vec").put("properties", new JSONArray().put("vector")).put("pqSubspaces", -1)
          .put("pqClusters", 256));
      assertThat(metadata.pqSubspaces).isEqualTo(-1);
      assertThat(metadata.pqClusters).isEqualTo(256);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource(delimiter = '|', quoteCharacter = '`', value = {
        // the issue's own value, and every other creation invariant the load used to skip
        "\"pqClusters\":16|\"pqClusters\":512|pqClusters cannot exceed 256",
        "\"pqClusters\":16|\"pqClusters\":0|pqClusters must be at least 1",
        "\"maxConnections\":32|\"maxConnections\":0|maxConnections must be at least 1",
        "\"beamWidth\":100|\"beamWidth\":0|beamWidth must be at least 1",
        "\"efSearch\":100|\"efSearch\":0|efSearch must be at least 1",
        "\"neighborOverflowFactor\":1.2|\"neighborOverflowFactor\":0.5|neighborOverflowFactor must be at least 1.0",
        "\"alphaDiversityRelaxation\":1.2|\"alphaDiversityRelaxation\":0.5|alphaDiversityRelaxation must be at least 1.0",
        "\"pqTrainingLimit\":128000|\"pqTrainingLimit\":0|pqTrainingLimit must be at least 1",
        // values the load already refused, which took the rest of the schema load down with them
        "\"similarityFunction\":\"COSINE\"|\"similarityFunction\":\"MANHATTAN\"|Invalid similarity function",
        "\"rerankOversample\":|\"rerankOversample\":-1,\"was\":|rerankOversample must be at least 0" })
    void aRefusedPersistedDefinitionLeavesOutItsIndexOnly(final String original, final String tampered, final String reason)
        throws IOException {
      createSchema();
      tamperSchemaFile(original, tampered);

      database = factory.open();

      // the vector index is left out...
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).as("the index whose definition is refused (%s)", reason).isFalse();
      // ...and nothing else: the index loaded after it still enforces uniqueness
      assertThat(database.getSchema().existsIndex(UNIQUE_INDEX)).as("an unrelated index loaded after the refused one").isTrue();
      database.transaction(() -> database.newDocument("Zzz").set("name", "a").save());
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Zzz").set("name", "a").save()))
          .isInstanceOf(DuplicatedKeyException.class);

      // queries on the type scan instead
      database.transaction(() -> database.newDocument("Vec").set("vector", new float[] { 1, 0, 0, 0 }).save());
      try (final ResultSet rs = database.query("sql", "SELECT count() AS n FROM Vec")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(4L);
      }

      // the definition is kept as it was: a save of an unrelated change does not drop it
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      database.close();
      assertThat(readSchemaFile()).as("the refused definition stays in schema.json").contains(tampered);
      database = factory.open();
    }

    @Test
    void correctingTheDefinitionBringsTheIndexBackAsItWas() throws IOException {
      createSchema();
      tamperSchemaFile("\"pqClusters\":16", "\"pqClusters\":512");

      database = factory.open();
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).isFalse();
      database.close();

      // the file of the index was kept, so correcting schema.json is all it takes
      tamperSchemaFile("\"pqClusters\":512", "\"pqClusters\":16");
      database = factory.open();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX);
      for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
        assertThat(((LSMVectorIndex) bucketIndex).getMetadata().pqClusters).isEqualTo(16);
      assertThat(typeIndex.countEntries()).as("the vectors indexed before the definition was refused").isEqualTo(3);
    }

    @Test
    void creatingTheIndexAgainReplacesTheRefusedDefinition() throws IOException {
      createSchema();
      tamperSchemaFile("\"pqClusters\":16", "\"pqClusters\":512");

      database = factory.open();
      // the bucket index left out by this open, whose file is kept
      final String refusedFile = vectorIndexEntryName();
      assertThat(database.getSchema().getIndexes()).extracting(Index::getName).doesNotContain(refusedFile);
      database.command("sql",
          "CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', quantization: 'PRODUCT', pqClusters: 32}");
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).isTrue();
      database.close();

      assertThat(readSchemaFile()).doesNotContain("\"pqClusters\":512").contains("\"pqClusters\":32");
      database = factory.open();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX);
      assertThat(typeIndex.countEntries()).isEqualTo(3);
      // the file of the replaced index is dropped by that open, not left as a type-less index for good
      for (final Index index : database.getSchema().getIndexes())
        assertThat(index.getTypeName()).as("index %s belongs to a type", index.getName()).isNotNull();
      assertThat(database.getSchema().getIndexes()).hasSize(4); // two type indexes, one bucket index each
      database.close();
      final String[] leftovers = new File(getDatabasePath()).list((dir, name) -> name.startsWith(refusedFile));
      assertThat(leftovers).as("files of the replaced index %s", refusedFile).isEmpty();
      database = factory.open();
    }

    @Test
    void aRefusedDefinitionMetByTheOrphanRelinkingPassKeepsItsFile() throws IOException {
      createSchema();
      // An entry keyed by a name that is no file's: the loader reaches the index file through the orphan-relinking pass (by its
      // bucket prefix), which is how it meets an index file a compaction renamed, and that pass applies the definition too
      final String fileName = vectorIndexEntryName();
      final String renamed = fileName.substring(0, fileName.lastIndexOf('_')) + "_1";
      tamperSchemaFile("\"" + fileName + "\":{", "\"" + renamed + "\":{");
      tamperSchemaFile("\"pqClusters\":16", "\"pqClusters\":512");

      database = factory.open();
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).isFalse();
      assertThat(database.getSchema().existsIndex(UNIQUE_INDEX)).isTrue();
      assertThat(database.getSchema().getIndexes()).extracting(Index::getName).doesNotContain(fileName);
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      database.close();

      // the definition is kept with its index kind, and the file is not swept as unreferenced
      final JSONObject kept = new JSONObject(readSchemaFile()).getJSONObject("types").getJSONObject("Vec").getJSONObject("indexes")
          .getJSONObject(renamed);
      assertThat(kept.getString("type")).isEqualTo("LSM_VECTOR");
      assertThat(kept.getInt("pqClusters")).isEqualTo(512);
      assertThat(new File(getDatabasePath()).list((dir, name) -> name.startsWith(fileName + "."))).as("the file of %s", fileName)
          .isNotEmpty();

      // corrected, the definition is relinked to the file it describes, with the vectors it holds
      tamperSchemaFile("\"pqClusters\":512", "\"pqClusters\":16");
      database = factory.open();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX);
      assertThat(typeIndex.countEntries()).isEqualTo(3);
    }

    @Test
    void aDefinitionRefusedByAnIncrementalReloadLeavesTheLiveIndexUntouchedUntilThePublication() throws IOException {
      createSchema();
      database = factory.open();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX);
      final LSMVectorIndex live = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      final String fileName = live.getName();

      // "similarityFunction" is read before "pqClusters": applied in place, the refusal used to leave the live index searching
      // with the new metric and the old graph
      tamperSchemaFile("\"similarityFunction\":\"COSINE\"", "\"similarityFunction\":\"EUCLIDEAN\"");
      tamperSchemaFile("\"pqClusters\":16", "\"pqClusters\":512");
      assertThat(database.getSchema().getEmbedded().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();

      assertThat(live.getMetadata().similarityFunction).as("the settings of the instance searches were using")
          .isEqualTo(VectorSimilarityFunction.COSINE);
      assertThat(live.getMetadata().pqClusters).isEqualTo(16);
      // withdrawn by the publication, with the rest of the schema served
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).isFalse();
      assertThat(database.getSchema().getIndexes()).extracting(Index::getName).doesNotContain(fileName);
      assertThat(database.getSchema().existsIndex(UNIQUE_INDEX)).isTrue();
    }

    @Test
    void anOrphanDefinitionRefusedByTheRelinkingPassKeepsItsIndexKind() throws IOException {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Article BUCKETS 1;
          CREATE PROPERTY Article.body STRING;
          CREATE INDEX ON Article (body) FULL_TEXT;""");
      database.close();

      // keyed by a name that is no file's, so the relinking pass meets it, with a null strategy the load refuses
      final JSONObject schema = new JSONObject(readSchemaFile());
      final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Article").getJSONObject("indexes");
      final String fileName = indexes.keySet().iterator().next();
      final JSONObject definition = indexes.getJSONObject(fileName);
      assertThat(definition.getString("type")).isEqualTo("FULL_TEXT");
      indexes.remove(fileName);
      final String renamed = fileName.substring(0, fileName.lastIndexOf('_')) + "_1";
      indexes.put(renamed, definition.put("nullStrategy", "NOT_A_STRATEGY"));
      Files.writeString(schemaFile().toPath(), schema.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      assertThat(database.getSchema().existsIndex("Article[body]")).isFalse();
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      database.close();

      // kept as a full-text definition, not as the LSM tree its file is
      final JSONObject kept = new JSONObject(readSchemaFile()).getJSONObject("types").getJSONObject("Article").getJSONObject("indexes")
          .getJSONObject(renamed);
      assertThat(kept.getString("type")).isEqualTo("FULL_TEXT");
      assertThat(kept.getString("nullStrategy")).isEqualTo("NOT_A_STRATEGY");
      database = factory.open();
    }

    @Test
    void droppingAProductQuantizedIndexLeavesNoFileBehind() throws IOException {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Vec BUCKETS 1;
          CREATE PROPERTY Vec.vector ARRAY_OF_FLOATS;
          CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN', quantization: 'PRODUCT', pqClusters: 16};""");
      database.transaction(() -> {
        for (int i = 0; i < 64; i++)
          database.newDocument("Vec").set("vector", new float[] { i, i % 7, i % 3, 1 }).save();
      });
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX);
      final LSMVectorIndex bucketIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      final String fileName = bucketIndex.getName();
      bucketIndex.buildVectorGraphNow();
      assertThat(new File(getDatabasePath()).list((dir, name) -> name.startsWith(fileName) && name.endsWith(".vecpq")))
          .as("the fixture builds a PQ file").isNotEmpty();
      final String graphFileName = fileName + "_" + LSMVectorIndexGraphFile.FILE_EXT;
      assertThat(((DatabaseInternal) database).getFileManager().getFileByComponentName(graphFileName)).as("the fixture builds a graph file").isNotNull();

      database.command("sql", "DROP INDEX `" + VECTOR_INDEX + "`");

      assertThat(((DatabaseInternal) database).getFileManager().getFileByComponentName(graphFileName)).as("the graph file stays registered").isNull();
      assertThat(new File(getDatabasePath()).list((dir, name) -> name.startsWith(fileName))).as("files of the dropped %s", fileName)
          .isEmpty();

      // and the database reopens without it, with the records the index held, and takes a new one on the same property
      database.close();
      database = factory.open();
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).isFalse();
      try (final ResultSet rs = database.query("sql", "SELECT count() AS n FROM Vec")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(64L);
      }
      database.command("sql",
          "CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN', quantization: 'PRODUCT', pqClusters: 16}");
      assertThat(((TypeIndex) database.getSchema().getIndexByName(VECTOR_INDEX)).countEntries()).isEqualTo(64);
    }

    @Test
    void aRefusedUniqueDefinitionIsReportedAsAUniqueIndexThatEnforcesNothing() throws IOException {
      createSchema();
      // a null strategy the LSM tree refuses
      final JSONObject schema = new JSONObject(readSchemaFile());
      final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Zzz").getJSONObject("indexes");
      final JSONObject definition = indexes.getJSONObject(indexes.keySet().iterator().next());
      assertThat(definition.getBoolean("unique")).isTrue();
      definition.put("nullStrategy", "NOT_A_STRATEGY");
      Files.writeString(schemaFile().toPath(), schema.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      assertThat(database.getSchema().existsIndex(UNIQUE_INDEX)).isFalse();
      assertThat(database.getSchema().existsIndex(VECTOR_INDEX)).as("the other indexes still load").isTrue();
      // listed with the unique indexes that enforce nothing, which an HA node answers with one re-install from its cluster: that
      // repairs a definition edited on this node alone
      assertThat(database.getSchema().getEmbedded().getUnavailableUniqueIndexNames()).hasSize(1);
    }

    @Test
    void aRefusedUniqueDefinitionOfOneBucketIsReportedAlthoughTheOtherBucketsLoad() throws IOException {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Multi BUCKETS 2;
          CREATE PROPERTY Multi.code STRING;
          CREATE INDEX ON Multi (code) UNIQUE;""");
      database.close();

      // one of the two bucket indexes refused: the type index of the other one still loads, incomplete
      final JSONObject schema = new JSONObject(readSchemaFile());
      final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Multi").getJSONObject("indexes");
      assertThat(indexes.length()).isEqualTo(2);
      indexes.getJSONObject(indexes.keySet().iterator().next()).put("nullStrategy", "NOT_A_STRATEGY");
      Files.writeString(schemaFile().toPath(), schema.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      final TypeIndex incomplete = (TypeIndex) database.getSchema().getIndexByName("Multi[code]");
      assertThat(incomplete.isValid()).as("the type index of the bucket that loaded covers only its own bucket").isFalse();
      assertThat(database.getSchema().getEmbedded().getUnavailableUniqueIndexNames())
          .as("an incomplete type index does not stand in for the refused one").hasSize(1);

      // corrected, or the integrity check after every test reports the incomplete index this test builds on purpose
      database.close();
      tamperSchemaFile("\"nullStrategy\":\"NOT_A_STRATEGY\"", "\"nullStrategy\":\"SKIP\"");
      database = factory.open();
      assertThat(((TypeIndex) database.getSchema().getIndexByName("Multi[code]")).isValid()).isTrue();
    }

    private String vectorIndexEntryName() throws IOException {
      return new JSONObject(readSchemaFile()).getJSONObject("types").getJSONObject("Vec").getJSONObject("indexes").keySet().iterator()
          .next();
    }

    private void createSchema() {
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Vec BUCKETS 1;
          CREATE PROPERTY Vec.vector ARRAY_OF_FLOATS;
          CREATE INDEX ON Vec (vector) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', quantization: 'PRODUCT', pqClusters: 16};
          CREATE DOCUMENT TYPE Zzz BUCKETS 1;
          CREATE PROPERTY Zzz.name STRING;
          CREATE INDEX ON Zzz (name) UNIQUE;""");
      database.transaction(() -> {
        database.newDocument("Vec").set("vector", new float[] { 1, 0, 0, 0 }).save();
        database.newDocument("Vec").set("vector", new float[] { 0, 1, 0, 0 }).save();
        database.newDocument("Vec").set("vector", new float[] { 0, 0, 1, 0 }).save();
      });
      database.close();
    }

    private void tamperSchemaFile(final String original, final String tampered) throws IOException {
      final String schema = readSchemaFile();
      assertThat(schema).as("the persisted definition carries %s", original).contains(original);
      Files.writeString(schemaFile().toPath(), schema.replace(original, tampered), StandardCharsets.UTF_8);
    }

    private String readSchemaFile() throws IOException {
      return Files.readString(schemaFile().toPath(), StandardCharsets.UTF_8);
    }

    private File schemaFile() {
      return new File(getDatabasePath(), "schema.json");
    }
  }

  /**
   * Regression for #6359: vector index wrapper.
   * <p>
   * Regression test for issue #6359: a vector bucket sub-index has to answer which {@link TypeIndex} it belongs to.
   * <p>
   * {@link LSMVectorIndex#getTypeIndex()} used to answer {@code null}, with the comment "Not applicable for this index
   * type", which was never true - a vector index IS registered under a wrapper, and every sibling family
   * ({@code LSMSparseVectorIndex}, {@code LSMTreeIndex}, {@code HashIndex}, ...) has always wired it.
   * <p>
   * Asserted here on the DROP path rather than through the {@code addSuperType} scenario that first exposed it, because
   * this half is the wider one: {@code LocalSchema.dropIndexInternal} skips {@code parentTypeIndex.removeIndexOnBucket}
   * when the answer is null, so EVERY path that drops a vector sub-index used to leave the wrapper holding a reference
   * to an index that no longer exists.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorIndexWrapper extends TestHelper {

    /** Two buckets, so dropping one sub-index leaves the wrapper alive to be asked about the other. */
    @Test
    @Timeout(60)
    void aVectorSubIndexNamesItsWrapperAndIsReleasedByItOnDrop() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("V", 2).createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql",
            "CREATE INDEX ON V (embedding) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"cosine\"}").close();
      });

      final TypeIndex wrapper = (TypeIndex) database.getSchema().getIndexByName("V[embedding]");
      final IndexInternal[] subIndexes = wrapper.getIndexesOnBuckets();
      assertThat(subIndexes).as("one sub-index per bucket").hasSize(2);

      for (final IndexInternal sub : subIndexes)
        assertThat(sub.getTypeIndex()).as("a vector sub-index names the wrapper it was registered under").isSameAs(wrapper);

      final IndexInternal dropped = subIndexes[0];
      database.getSchema().dropIndex(dropped.getName());

      assertThat(wrapper.getIndexesOnBuckets()).as("and the wrapper lets go of it when it is dropped")
          .doesNotContain(dropped).hasSize(1);

      // a type index left without the bucket index of one of its buckets is reported by CHECK DATABASE, run after each test
      database.getSchema().dropIndex(wrapper.getName());
    }
  }

  /**
   * Regression for #10438: vector index created in writing script.
   * <p>
   * Regression test for issue #10438: an {@code LSM_VECTOR} index created in the same transaction as the writes it should
   * cover - the shape of a single {@code sqlscript} request over HTTP, which the handler wraps in one transaction -
   * answered every search with no rows. The vector families were excluded from sharing the caller's transaction
   * ({@code Schema.INDEX_TYPE.buildCanShareCallerTransaction}), so their build ran in a nested transaction of its own,
   * which cannot see the caller's uncommitted inserts (reported as {@code totalIndexed: 0}) and reads the committed image
   * of records the caller has updated (reported as {@code totalIndexed: 3} with no vector to index). The caller's writes
   * predate the index, so none of them had queued an index entry either: the index stayed empty after the commit.
   * <p>
   * One test per entry point that decides whether a build shares the caller's transaction: the type-level builder
   * (SQL {@code CREATE INDEX} and the Java API), the bucket-level builder, and the propagation of a super type's indexes.
   */
  @Nested
  class VectorIndexCreatedInWritingScript extends TestHelper {

    private static final float[] QUERY = { 0.9f, 0.1f, 0.0f };

    /** The second reproduction of the issue: records inserted with their vectors, then the index, in one script. */
    @Test
    void indexCreatedAfterInsertsInSameScriptIsSearchable() {
      final ResultSet created = database.command("sqlscript", """
          CREATE VERTEX TYPE Acc3; CREATE PROPERTY Acc3.behavior ARRAY_OF_FLOATS;
          BEGIN;
          INSERT INTO Acc3 SET behavior=[0.9,0.1,0.0]; INSERT INTO Acc3 SET behavior=[0.1,0.9,0.0];
          CREATE INDEX ON Acc3 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'};
          COMMIT;
          """);
      assertThat(created.hasNext()).isTrue();

      assertEntries("Acc3[behavior]", 2);
      assertThat(neighbors("Acc3[behavior]")).hasSize(2);
    }

    /** The first reproduction of the issue: vectors set by UPDATE on committed records, then the index, in one script. */
    @Test
    void indexCreatedAfterUpdatesInSameScriptIsSearchable() {
      database.command("sqlscript",
          "CREATE VERTEX TYPE Acc; BEGIN; INSERT INTO Acc SET id='A'; INSERT INTO Acc SET id='B'; INSERT INTO Acc SET id='X'; COMMIT;");
      database.command("sqlscript", """
          CREATE PROPERTY Acc.behavior ARRAY_OF_FLOATS;
          BEGIN;
          UPDATE Acc SET behavior = [0.9, 0.1, 0.0] WHERE id IN ['A','B'];
          UPDATE Acc SET behavior = [0.85, 0.15, 0.05] WHERE id = 'X';
          CREATE INDEX ON Acc (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'};
          COMMIT;
          """);

      assertEntries("Acc[behavior]", 3);
      assertThat(neighbors("Acc[behavior]")).hasSize(3);
    }

    /** The totalIndexed the CREATE INDEX reports must agree with what the index can find. */
    @Test
    void totalIndexedAgreesWithTheSearch() {
      database.command("sql", "CREATE VERTEX TYPE Acc5");
      database.command("sql", "CREATE PROPERTY Acc5.behavior ARRAY_OF_FLOATS");
      final long[] totalIndexed = new long[1];
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Acc5 SET behavior=[0.9,0.1,0.0]");
        database.command("sql", "INSERT INTO Acc5 SET behavior=[0.1,0.9,0.0]");
        try (final ResultSet rs = database.command("sql",
            "CREATE INDEX ON Acc5 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}")) {
          totalIndexed[0] = ((Number) rs.next().getProperty("totalIndexed")).longValue();
        }
      });

      assertThat(totalIndexed[0]).isEqualTo(2L);
      assertEntries("Acc5[behavior]", 2);
      assertThat(neighbors("Acc5[behavior]")).hasSize(2);
    }

    /** The index sees the caller's own writes before the commit too (the overlay of issue #7378). */
    @Test
    void indexCreatedInsideTransactionAnswersBeforeTheCommit() {
      database.command("sql", "CREATE VERTEX TYPE Acc6");
      database.command("sql", "CREATE PROPERTY Acc6.behavior ARRAY_OF_FLOATS");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Acc6 SET behavior=[0.9,0.1,0.0]");
        database.command("sql", "INSERT INTO Acc6 SET behavior=[0.1,0.9,0.0]");
        database.command("sql", "CREATE INDEX ON Acc6 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");

        assertThat(neighbors("Acc6[behavior]")).hasSize(2);
      });

      assertThat(neighbors("Acc6[behavior]")).hasSize(2);
    }

    /** A record the caller deleted in the same transaction must not be indexed from its committed image. */
    @Test
    void recordDeletedInSameTransactionIsNotIndexed() {
      database.command("sql", "CREATE VERTEX TYPE Acc7");
      database.command("sql", "CREATE PROPERTY Acc7.behavior ARRAY_OF_FLOATS");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Acc7 SET id = 'keep', behavior=[0.9,0.1,0.0]");
        database.command("sql", "INSERT INTO Acc7 SET id = 'gone', behavior=[0.1,0.9,0.0]");
      });

      database.transaction(() -> {
        database.command("sql", "DELETE FROM Acc7 WHERE id = 'gone'");
        database.command("sql", "CREATE INDEX ON Acc7 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      });

      final List<Result> rows = neighbors("Acc7[behavior]");
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<String>getProperty("id")).isEqualTo("keep");
      assertThat(database.getSchema().getIndexByName("Acc7[behavior]").countEntries()).isEqualTo(1L);
    }

    /**
     * Committed records are indexed exactly once when the index is built inside a transaction holding unrelated writes.
     * Such a build shares the transaction, so its entries are still queued when {@code CREATE INDEX} returns: the eager
     * graph build the statement used to run right after it found no entry on the index pages, recovered the records
     * through its document-scan fallback, and the commit then added every one of them a second time.
     */
    @Test
    void committedRecordsAreIndexedOnceWhenTheTransactionHoldsUnrelatedWrites() {
      database.command("sql", "CREATE DOCUMENT TYPE Acc9");
      database.command("sql", "CREATE PROPERTY Acc9.behavior ARRAY_OF_FLOATS");
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Acc9 SET behavior=[0.9,0.1,0.0]");
        database.command("sql", "INSERT INTO Acc9 SET behavior=[0.1,0.9,0.0]");
      });

      database.transaction(() -> {
        database.command("sql", "INSERT INTO Unrelated SET x = 1");
        database.command("sql", "INSERT INTO Acc9 SET behavior=[0.5,0.5,0.0]");
        database.command("sql", "CREATE INDEX ON Acc9 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      });

      assertEntries("Acc9[behavior]", 3);
      assertThat(neighbors("Acc9[behavior]")).hasSize(3);

      reopenDatabase();
      assertEntries("Acc9[behavior]", 3);
      assertThat(neighbors("Acc9[behavior]")).hasSize(3);
    }

    /** A rolled-back script leaves the index with nothing of the rolled-back records, and usable for later writes. */
    @Test
    void rolledBackTransactionLeavesNoEntries() {
      database.command("sql", "CREATE VERTEX TYPE Acc8");
      database.command("sql", "CREATE PROPERTY Acc8.behavior ARRAY_OF_FLOATS");
      database.begin();
      database.command("sql", "INSERT INTO Acc8 SET behavior=[0.9,0.1,0.0]");
      database.command("sql", "CREATE INDEX ON Acc8 (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      database.rollback();

      assertThat(neighbors("Acc8[behavior]")).isEmpty();

      database.transaction(() -> database.command("sql", "INSERT INTO Acc8 SET behavior=[0.9,0.1,0.0]"));
      assertThat(neighbors("Acc8[behavior]")).hasSize(1);
    }

    /** A type with several buckets: every bucket's sub-index covers the records the transaction wrote into it. */
    @Test
    void multiBucketTypeIndexCreatedAfterInsertsInSameTransactionCoversEveryBucket() {
      final DocumentType type = database.getSchema().createDocumentType("MultiDoc", 3);
      type.createProperty("behavior", Type.ARRAY_OF_FLOATS);

      database.transaction(() -> {
        for (int i = 0; i < 9; i++)
          database.newDocument("MultiDoc").set("behavior", new float[] { 0.9f, 0.1f, i / 10f }).save();
        database.command("sql", "CREATE INDEX ON MultiDoc (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("MultiDoc[behavior]");
      assertThat(typeIndex.getIndexesOnBuckets()).hasSize(3);
      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
        assertThat(bucketIndex.countEntries()).as("entries of %s", bucketIndex.getName())
            .isEqualTo(database.countBucket(database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName()));
      assertEntries("MultiDoc[behavior]", 9);

      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT expand(vectorNeighbors('MultiDoc[behavior]', [0.9,0.1,0.0], 9))")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      assertThat(rows).hasSize(9);
    }

    /** Bucket-level builder: the second caller of the shared-transaction decision. */
    @Test
    void bucketIndexCreatedAfterInsertsInSameTransactionIsSearchable() {
      final DocumentType type = database.getSchema().createDocumentType("BucketDoc", 1);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      final String bucketName = type.getBuckets(false).getFirst().getName();

      final LSMVectorIndex[] index = new LSMVectorIndex[1];
      database.transaction(() -> {
        database.newDocument("BucketDoc").set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
        database.newDocument("BucketDoc").set("embedding", new float[] { 0.1f, 0.9f, 0.0f }).save();

        final BucketLSMVectorIndexBuilder builder = (BucketLSMVectorIndexBuilder) database.getSchema()
            .buildBucketIndex("BucketDoc", bucketName, new String[] { "embedding" })
            .withType(Schema.INDEX_TYPE.LSM_VECTOR);
        builder.withDimensions(3).withSimilarity("COSINE");
        index[0] = (LSMVectorIndex) builder.create();
      });

      assertThat(index[0].countEntries()).isEqualTo(2L);
      final List<Pair<RID, Float>> found = index[0].findNeighborsFromVector(QUERY, 3);
      assertThat(found).hasSize(2);
    }

    /** Super-type propagation: the third caller of the shared-transaction decision (issue #6359). */
    @Test
    void indexPropagatedFromSuperTypeInSameTransactionCoversTheWrites() {
      database.command("sql", "CREATE DOCUMENT TYPE Parent");
      database.command("sql", "CREATE PROPERTY Parent.behavior ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Parent (behavior) LSM_VECTOR METADATA {dimensions: 3, similarity: 'COSINE'}");
      database.command("sql", "CREATE DOCUMENT TYPE Child");
      database.command("sql", "CREATE PROPERTY Child.behavior ARRAY_OF_FLOATS");

      database.transaction(() -> {
        final MutableDocument first = database.newDocument("Child");
        first.set("behavior", new float[] { 0.9f, 0.1f, 0.0f }).save();
        database.newDocument("Child").set("behavior", new float[] { 0.1f, 0.9f, 0.0f }).save();
        database.getSchema().getType("Child").addSuperType("Parent");
      });

      assertEntries("Parent[behavior]", 2);
      assertThat(neighbors("Parent[behavior]")).hasSize(2);
    }

    /** The sparse vector family carried the same exclusion. */
    @Test
    void sparseIndexCreatedAfterInsertsInSameTransactionIsSearchable() {
      final DocumentType type = database.getSchema().createDocumentType("SparseDoc");
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.transaction(() -> {
        database.newDocument("SparseDoc").set("tokens", new int[] { 3, 10 }).set("weights", new float[] { 1.0f, 0.5f }).save();
        database.newDocument("SparseDoc").set("tokens", new int[] { 10, 16 }).set("weights", new float[] { 0.5f, 0.25f }).save();

        database.getSchema().buildTypeIndex("SparseDoc", new String[] { "tokens", "weights" })
            .withSparseVectorType()
            .withDimensions(50)
            .create();
      });

      assertThat(database.getSchema().getIndexByName("SparseDoc[tokens,weights]").countEntries())
          .as("a sparse index counts postings: two records of two dimensions each").isEqualTo(4L);

      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
          "SparseDoc[tokens,weights]", new int[] { 3, 10, 16 }, new float[] { 1.0f, 0.5f, 0.25f }, 10)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      assertThat(rows).hasSize(2);
    }

    /**
     * Asserted BEFORE any search: a search over an index whose graph is not built yet can rebuild an in-memory graph
     * from the bucket, which answers that one search while nothing was ever persisted in the index (a reopen finds it
     * empty again).
     */
    private void assertEntries(final String indexName, final long expected) {
      assertThat(database.getSchema().getIndexByName(indexName).countEntries()).isEqualTo(expected);
    }

    private List<Result> neighbors(final String indexName) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorNeighbors('" + indexName + "', [0.9,0.1,0.0], 3))")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #10021: schema reload warm up.
   * <p>
   * Issue #10021: a full schema load - which an HA replica runs for every schema entry its incremental refresh declines -
   * re-instantiates every component, and an {@code LSM_VECTOR} index came back with nothing resident: the next search read
   * all of its pages to rebuild the location map and then loaded or rebuilt the graph, on the query thread, under the
   * JVM-wide rebuild permit. One unrelated DDL turned the searches that followed into a burst of those, on every vector index.
   * <p>
   * The reload now warms, in the background, the indexes whose replaced instance had a graph resident, and only those.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class SchemaReloadWarmUp {
    private static final String DB_PATH       = "target/test-databases/Issue10021SchemaReloadWarmUpTest";
    private static final int    DIMENSIONS    = 16;
    private static final int    NUM_VECTORS   = 1_000;
    /** A bound for waiting on something that must happen, sized as a hang detector and not as a latency claim. */
    private static final long   AWAIT_MS      = 120_000L;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(0);
      build();
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aWarmIndexIsWarmedInTheBackgroundAfterAFullSchemaLoad() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex before = vectorIndex(db);
          before.warmUp();
          assertThat(before.isWarm()).isTrue();
          assertThat(before.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

          fullSchemaLoad(db);

          final LSMVectorIndex after = vectorIndex(db);
          assertThat(after).as("the reload re-creates the index").isNotSameAs(before);
          assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();

          assertThat(after.isWarm()).as("warmed without a search having asked for it").isTrue();
          assertThat(after.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
          assertThat(after.getStats().get("graphRebuildCount")).as("loaded from the persisted graph, not rebuilt").isZero();
          // The first search finds everything resident: it loads and builds nothing
          final long loadsBefore = after.getStats().get("graphLoadsFromOrdinalMap");
          assertThat(after.findNeighborsFromVector(embedding(3), 3, 64)).hasSize(3);
          assertThat(after.getStats().get("graphLoadsFromOrdinalMap")).isEqualTo(loadsBefore);
          assertThat(after.getStats().get("graphRebuildCount")).isZero();
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void anIndexNobodySearchedIsNotWarmed() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          assertThat(vectorIndex(db).isWarm()).as("a freshly opened database has nothing resident").isFalse();

          fullSchemaLoad(db);

          assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
          final LSMVectorIndex after = vectorIndex(db);
          assertThat(after.isWarm()).as("warming it would charge the replica for an index nobody queries").isFalse();
          assertThat(after.getStats().get("graphNodeCount")).isZero();
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void theWarmUpCanBeSwitchedOff() throws Exception {
      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(-1);
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          vectorIndex(db).warmUp();

          fullSchemaLoad(db);

          assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
          assertThat(vectorIndex(db).isWarm()).as("left to the first search, as before").isFalse();
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void anIndexReCreatedTwiceBeforeItGotWarmStaysInTheChain() throws Exception {
      // The first reload's warm-up is held back by a delay that never elapses in the test; the second reload, which finds an
      // instance that is not warm because the warm-up has not run, must still ask for its successor to be warmed
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          vectorIndex(db).warmUp();

          GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
          fullSchemaLoad(db);
          assertThat(vectorIndex(db).isWarm()).as("the first reload's warm-up is still waiting").isFalse();

          GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(0);
          fullSchemaLoad(db);

          assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
          assertThat(vectorIndex(db).isWarm()).as("the intent survived the second reload").isTrue();
          assertThat(vectorIndex(db).getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void closingTheDatabaseWhileAWarmUpWaitsIsClean() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        vectorIndex(db).warmUp();

        GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
        fullSchemaLoad(db);

        final LocalSchema schema = schema(db);
        db.close();

        assertThat(schema.awaitIndexWarmUp(AWAIT_MS)).as("the waiting warm-up was dropped with the schema").isTrue();
      }
    }

    private static LocalSchema schema(final Database db) {
      return ((DatabaseInternal) db).getSchema().getEmbedded();
    }

    private static void fullSchemaLoad(final Database db) throws Exception {
      schema(db).load(ComponentFile.MODE.READ_WRITE, true);
    }

    private static void build() {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++) {
            db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
            if (i % 500 == 499) {
              db.commit();
              db.begin();
            }
          }
          db.commit();
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x10021L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
          .getIndexesOnBuckets()[0];
    }
  }
}
