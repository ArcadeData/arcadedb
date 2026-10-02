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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8958: a vector search through the index of a parent type must cover the records of its sub-types too (the cross-type
 * search), while a sub-type spec keeps returning only the records of that sub-type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexParentTypeSearchTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Embedding");
      database.command("sql", "CREATE PROPERTY Embedding.name STRING");
      database.command("sql", "CREATE PROPERTY Embedding.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
      database.command("sql", "CREATE VERTEX TYPE EmbeddingImage EXTENDS Embedding");
      database.command("sql", "CREATE VERTEX TYPE EmbeddingText EXTENDS Embedding");
      database.command("sql", "CREATE VERTEX TYPE EmbeddingIcon EXTENDS EmbeddingImage");
    });
    database.transaction(() -> {
      database.newVertex("Embedding").set("name", "parent", "vector", new float[] { 0.5f, 0.5f }).save();
      database.newVertex("EmbeddingImage").set("name", "image1", "vector", new float[] { 0.9f, 0.1f }).save();
      database.newVertex("EmbeddingImage").set("name", "image2", "vector", new float[] { 0.8f, 0.2f }).save();
      database.newVertex("EmbeddingIcon").set("name", "icon1", "vector", new float[] { 0.7f, 0.3f }).save();
      database.newVertex("EmbeddingText").set("name", "text1", "vector", new float[] { 0.2f, 0.8f }).save();
      database.newVertex("EmbeddingText").set("name", "text2", "vector", new float[] { 0.1f, 0.9f }).save();
    });
  }

  @Test
  void sqlParentSpecCoversSubTypes() {
    assertThat(sqlNames("Embedding")).containsExactlyInAnyOrder("parent", "image1", "image2", "icon1", "text1", "text2");
  }

  @Test
  void sqlSubTypeSpecKeepsItsOwnRecordsAndTheirSubTypes() {
    assertThat(sqlNames("EmbeddingText")).containsExactlyInAnyOrder("text1", "text2");
    assertThat(sqlNames("EmbeddingImage")).containsExactlyInAnyOrder("image1", "image2", "icon1");
    assertThat(sqlNames("EmbeddingIcon")).containsExactlyInAnyOrder("icon1");
  }

  @Test
  void cypherParentSpecCoversSubTypes() {
    assertThat(cypherNames("Embedding")).containsExactlyInAnyOrder("parent", "image1", "image2", "icon1", "text1", "text2");
    assertThat(cypherNames("EmbeddingText")).containsExactlyInAnyOrder("text1", "text2");
    assertThat(cypherNames("EmbeddingImage")).containsExactlyInAnyOrder("image1", "image2", "icon1");
  }

  @Test
  void parentHoldingNoRecordsStillAnswers() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Embedding WHERE name = 'parent'");
    });
    assertThat(sqlNames("Embedding")).containsExactlyInAnyOrder("image1", "image2", "icon1", "text1", "text2");
    assertThat(cypherNames("Embedding")).containsExactlyInAnyOrder("image1", "image2", "icon1", "text1", "text2");
  }

  private List<String> sqlNames(final String type) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.neighbors`('" + type + "[vector]', ?, 10)))", (Object) new float[] { 1.0f, 0.0f })) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private List<String> cypherNames(final String type) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes('" + type + "[vector]', 10, $v) YIELD node RETURN node.name AS name",
        Map.of("v", new float[] { 1.0f, 0.0f }))) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
