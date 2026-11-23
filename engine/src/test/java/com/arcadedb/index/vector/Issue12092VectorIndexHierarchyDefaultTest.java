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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12092: an LSM_VECTOR index created without {@code addHierarchy} in its METADATA was a flat graph, every search
 * started from one entry node and on clustered data a few queries settled in the cluster next to the query's own. New
 * indexes are hierarchical by default; an index persisted before the change, whose definition may not carry the key,
 * keeps the flat graph it was built with.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12092VectorIndexHierarchyDefaultTest extends TestHelper {

  @Test
  void anSqlIndexWithoutTheKeyIsHierarchical() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");
    });

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isTrue());
  }

  @Test
  void aCypherIndexWithoutTheKeyIsHierarchical() {
    database.command("opencypher", "CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4}}");

    assertThat(bucketMetadata("v")).allSatisfy(m -> assertThat(m.addHierarchy).isTrue());
  }

  @Test
  void aJavaBuilderIndexWithoutTheSettingIsHierarchical() {
    database.getSchema().createVertexType("Doc");
    database.getSchema().getType("Doc").createProperty("embedding", float[].class);
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withDimensions(4).withSimilarity("COSINE");
    builder.create();

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isTrue());
  }

  @Test
  void anExplicitFalseIsHonouredAndSurvivesAReopen() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE', addHierarchy: false}");
    });
    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isFalse());

    reopenDatabase();

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isFalse());
  }

  @Test
  void aHierarchicalDefaultIndexSurvivesAReopenAndAnswers() {
    createDefaultIndexWithVectors();

    reopenDatabase();

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isTrue());
    assertThat(nearestId()).isEqualTo(0);
  }

  /**
   * The key has not always been persisted: a definition written without it describes an index built flat, and the new
   * default must not turn it hierarchical on its next rebuild.
   */
  @Test
  void aPersistedDefinitionWithoutTheKeyStaysFlat() throws IOException {
    createDefaultIndexWithVectors();
    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath, LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    assertThat(removeKey(root, "addHierarchy")).as("the schema carries the index definition the test rewrites").isPositive();
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

    reopenDatabase();

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isFalse());
    database.transaction(() -> database.newVertex("Doc").set("id", 99).set("embedding", new float[] { 0F, 0F, 0F, 1F }).save());
    assertThat(nearestId()).isEqualTo(0);
  }

  @Test
  void fromJsonReadsAnAbsentKeyAsFlatAndAnExplicitOneAsGiven() {
    final LSMVectorIndexMetadata legacy = new LSMVectorIndexMetadata("Doc", new String[] { "embedding" }, -1);
    legacy.fromJSON(definition().put("dimensions", 4));
    assertThat(legacy.addHierarchy).isFalse();

    final LSMVectorIndexMetadata explicit = new LSMVectorIndexMetadata("Doc", new String[] { "embedding" }, -1);
    explicit.fromJSON(definition().put("dimensions", 4).put("addHierarchy", true));
    assertThat(explicit.addHierarchy).isTrue();

    assertThat(new LSMVectorIndexMetadata("Doc", new String[] { "embedding" }, -1).addHierarchy).isTrue();
  }

  @Test
  void anImportedDefinitionWithoutTheKeyStaysFlat() {
    database.getSchema().createVertexType("Doc");
    database.getSchema().getType("Doc").createProperty("embedding", float[].class);
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType();
    builder.withPersistedMetadata(definition().put("dimensions", 4).put("similarityFunction", "COSINE"));
    builder.create();

    assertThat(bucketMetadata("Doc[embedding]")).allSatisfy(m -> assertThat(m.addHierarchy).isFalse());
  }

  /**
   * SHOW INDEXES lists the settings that differ from the creation default, so with the default flipped a flat index must
   * list {@code addHierarchy: false}, and the statement it prints must create a flat index again.
   */
  @Test
  void showIndexesNamesAFlatIndexSoItsStatementRecreatesIt() {
    database.command("opencypher",
        "CREATE VECTOR INDEX flat FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4, addHierarchy: false}}");
    database.command("opencypher", "CREATE VECTOR INDEX hier FOR (n:M) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4}}");

    final String flatStatement = createStatement("flat");
    assertThat(flatStatement).contains("`addHierarchy`: false");
    assertThat(createStatement("hier")).doesNotContain("addHierarchy");

    database.command("opencypher", "DROP INDEX flat");
    database.command("opencypher", flatStatement);
    assertThat(bucketMetadata("flat")).allSatisfy(m -> assertThat(m.addHierarchy).isFalse());
  }

  private String createStatement(final String name) {
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD * WHERE name = $name", "name", name)) {
      return rs.next().getProperty("createStatement");
    }
  }

  private static JSONObject definition() {
    return new JSONObject().put("typeName", "Doc").put("properties", new JSONArray().put("embedding"));
  }

  private void createDefaultIndexWithVectors() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'COSINE'}");
    });
    database.transaction(() -> {
      database.newVertex("Doc").set("id", 0).set("embedding", new float[] { 1F, 0F, 0F, 0F }).save();
      for (int i = 1; i < 50; i++)
        database.newVertex("Doc").set("id", i).set("embedding", new float[] { 0.1F, 1F, i / 50F, 0.5F }).save();
    });
  }

  private int nearestId() {
    try (final ResultSet rs = database.query("sql",
        "SELECT id FROM (SELECT expand(vector.neighbors('Doc[embedding]', [0.99, 0.01, 0.0, 0.0], 1)))")) {
      return rs.next().<Integer>getProperty("id");
    }
  }

  private List<LSMVectorIndexMetadata> bucketMetadata(final String indexName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final List<LSMVectorIndexMetadata> result = new ArrayList<>();
    for (final Index index : typeIndex.getIndexesOnBuckets())
      result.add(((LSMVectorIndex) index).getMetadata());
    assertThat(result).as("the index has a sub-index on every bucket of its type").isNotEmpty();
    return result;
  }

  private static int removeKey(final Object node, final String key) {
    int removed = 0;
    if (node instanceof JSONObject object) {
      if (object.has(key)) {
        object.remove(key);
        ++removed;
      }
      for (final String name : object.keySet())
        removed += removeKey(object.get(name), key);
    } else if (node instanceof JSONArray array) {
      for (int i = 0; i < array.length(); i++)
        removed += removeKey(array.get(i), key);
    }
    return removed;
  }
}
