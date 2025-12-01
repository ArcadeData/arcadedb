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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Test INSERT CONTENT with empty arrays - reproduces issue #2497.
 * <p>
 * This test reproduces the NoSuchElementException that occurs when executing
 * INSERT CONTENT SQL statements with documents containing empty arrays.
 *
 *
 */
public class InsertContentEmptyArrayTest extends TestHelper {

  public InsertContentEmptyArrayTest() {
    autoStartTx = true;
  }

  @Test
  void insertContentWithEmptyArray() {
    final String className = "DataCollectionEntity";
    database.getSchema().createDocumentType(className);

    // This is the exact failing SQL from issue #2497
    final String sql = """
        INSERT INTO DataCollectionEntity CONTENT
        {"type":"DOCUMENT","attributes":[],
        "created":"2025-09-10T14:40:14.723Z",
        "amended":"2025-09-10T14:40:14.723Z",
        "id":"119463d6-9b69-415c-94bc-9629e92d6560"}""";

    // This should not throw NoSuchElementException
    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("type")).isEqualTo("DOCUMENT");
      assertThat(item.<Object>getProperty("attributes")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("attributes")).isEmpty();
      assertThat(item.<String>getProperty("created")).isEqualTo("2025-09-10T14:40:14.723Z");
      assertThat(item.<String>getProperty("amended")).isEqualTo("2025-09-10T14:40:14.723Z");
      assertThat(item.<String>getProperty("id")).isEqualTo("119463d6-9b69-415c-94bc-9629e92d6560");
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void insertContentWithMultipleEmptyArrays() {
    final String className = "TestMultipleEmptyArrays";
    database.getSchema().createDocumentType(className);

    final String sql = """
        INSERT INTO TestMultipleEmptyArrays CONTENT
        {"name":"test","emptyArray1":[],
        "emptyArray2":[],"data":[1,2,3],
        "anotherEmpty":[]}""";

    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("test");

      // Verify empty arrays
      assertThat(item.<Object>getProperty("emptyArray1")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("emptyArray1")).isEmpty();
      assertThat(item.<Object>getProperty("emptyArray2")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("emptyArray2")).isEmpty();
      assertThat(item.<Object>getProperty("anotherEmpty")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("anotherEmpty")).isEmpty();

      // Verify non-empty array
      assertThat(item.<Object>getProperty("data")).isInstanceOf(List.class);
      List<Integer> dataList = item.getProperty("data");
      assertThat(dataList).containsExactly(1, 2, 3);

      result.close();
    });
  }

  @Test
  void insertContentWithNestedEmptyArrays() {
    final String className = "TestNestedEmptyArrays";
    database.getSchema().createDocumentType(className);

    final String sql = """
        INSERT INTO TestNestedEmptyArrays CONTENT
        {"nested":{"inner":[],"value":42},
        "outer":[]}""";

    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();

      // Verify outer empty array
      assertThat(item.<Object>getProperty("outer")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("outer")).isEmpty();

      // Verify nested structure
      assertThat(item.<Object>getProperty("nested")).isInstanceOf(Map.class);
      Map<?, ?> nested = item.<Map<?, ?>>getProperty("nested");
      assertThat(nested.get("inner")).isInstanceOf(List.class);
      assertThat(((List<?>) nested.get("inner"))).isEmpty();
      assertThat(nested.get("value")).isEqualTo(42);

      result.close();
    });
  }

  @Test
  void insertContentWithEmptyArrayAsParameter() {
    final String className = "TestEmptyArrayParam";
    database.getSchema().createDocumentType(className);

    // Test using parameter with empty array
    Map<String, Object> params = new HashMap<>();
    Map<String, Object> content = new HashMap<>();
    content.put("name", "paramTest");
    content.put("emptyList", new ArrayList<>());
    content.put("emptyArray", new Object[0]);
    params.put("content", content);

    final String sql = "INSERT INTO " + className + " CONTENT :content";

    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql, params);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("paramTest");

      // Verify empty collections
      assertThat(item.<Object>getProperty("emptyList")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("emptyList")).isEmpty();

      // Object[] arrays are stored as arrays, not converted to Lists
      assertThat(item.<Object>getProperty("emptyArray")).isInstanceOf(Object[].class);
      Object[] emptyArray = item.<Object[]>getProperty("emptyArray");
      assertThat(emptyArray).hasSize(0);

      result.close();
    });
  }

  @Test
  void insertContentWithOnlyEmptyArray() {
    final String className = "TestOnlyEmptyArray";
    database.getSchema().createDocumentType(className);

    final String sql = """
        INSERT INTO TestOnlyEmptyArray CONTENT {"items":[]}""";

    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Object>getProperty("items")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("items")).isEmpty();

      result.close();
    });
  }

  @Test
  void insertContentArrayOfArraysWithEmpty() {
    final String className = "TestArrayOfArrays";
    database.getSchema().createDocumentType(className);

    final String sql = """
        INSERT INTO TestArrayOfArrays CONTENT
        {"matrix":[[1,2],[],[3,4],[]]}""";

    assertThatNoException().isThrownBy(() -> {
      ResultSet result = database.command("sql", sql);

      assertThat(result.hasNext()).isTrue();
      Result item = result.next();
      assertThat(item).isNotNull();

      List<?> matrix = item.<List<?>>getProperty("matrix");
      assertThat(matrix).hasSize(4);

      // First array: [1,2]
      assertThat(matrix.get(0)).isInstanceOf(List.class);
      List<Integer> first = (List<Integer>) matrix.get(0);
      assertThat(first).containsExactly(1, 2);

      // Second array: empty
      assertThat(matrix.get(1)).isInstanceOf(List.class);
      List<?> second = (List<?>) matrix.get(1);
      assertThat(second).isEmpty();

      // Third array: [3,4]
      assertThat(matrix.get(2)).isInstanceOf(List.class);
      List<Integer> third = (List<Integer>) matrix.get(2);
      assertThat(third).containsExactly(3, 4);

      // Fourth array: empty
      assertThat(matrix.get(3)).isInstanceOf(List.class);
      List<?> fourth = (List<?>) matrix.get(3);
      assertThat(fourth).isEmpty();

      result.close();
    });
  }

  @Test
  void insertContentQueryAndRetrieve() {
    final String className = "TestQueryRetrieve";
    database.getSchema().createDocumentType(className);

    // Insert document with empty array
    final String insertSql = """
        INSERT INTO TestQueryRetrieve CONTENT
        {"name":"queryTest","tags":[]}
        """;

    assertThatNoException().isThrownBy(() -> {
      ResultSet insertResult = database.command("sql", insertSql);
      assertThat(insertResult.hasNext()).isTrue();
      insertResult.next();
      insertResult.close();

      // Query the inserted document
      ResultSet queryResult = database.query("sql", "SELECT FROM " + className + " WHERE name = 'queryTest'");
      assertThat(queryResult.hasNext()).isTrue();
      Result item = queryResult.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("queryTest");
      assertThat(item.<Object>getProperty("tags")).isInstanceOf(List.class);
      assertThat(item.<List<?>>getProperty("tags")).isEmpty();
      queryResult.close();
    });
  }
}
