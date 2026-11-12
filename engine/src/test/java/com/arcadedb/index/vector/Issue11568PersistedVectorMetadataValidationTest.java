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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue11568PersistedVectorMetadataValidationTest extends TestHelper {
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
