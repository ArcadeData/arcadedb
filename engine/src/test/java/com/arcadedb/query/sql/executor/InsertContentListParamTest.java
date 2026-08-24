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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces and verifies the fix for issue #6463: {@code INSERT INTO ... CONTENT :param} silently
 * inserted only the first element of a {@link List} bound parameter, unlike the equivalent JSON-array
 * literal form which creates one record per item.
 */
public class InsertContentListParamTest extends TestHelper {

  public InsertContentListParamTest() {
    autoStartTx = true;
  }

  @Test
  void insertContentWithListParamCreatesOneRecordPerItem() {
    final String typeName = "Issue6463Person";
    database.getSchema().createDocumentType(typeName);

    final List<Map<String, Object>> people = new ArrayList<>();
    for (final String name : List.of("a", "b", "c")) {
      final Map<String, Object> item = new LinkedHashMap<>();
      item.put("name", name);
      people.add(item);
    }

    final Map<String, Object> params = new HashMap<>();
    params.put("people", people);

    final ResultSet result = database.command("sql", "INSERT INTO " + typeName + " CONTENT :people", params);

    final List<String> insertedNames = new ArrayList<>();
    while (result.hasNext())
      insertedNames.add(result.next().<String>getProperty("name"));
    result.close();

    assertThat(insertedNames).containsExactlyInAnyOrder("a", "b", "c");

    final ResultSet count = database.query("sql", "SELECT count(*) as total FROM " + typeName);
    assertThat(count.hasNext()).isTrue();
    assertThat(count.next().<Long>getProperty("total")).isEqualTo(3L);
    count.close();
  }

  @Test
  void insertContentWithSingleElementListParamStillCreatesOneRecord() {
    final String typeName = "Issue6463SingleItem";
    database.getSchema().createDocumentType(typeName);

    final Map<String, Object> item = new LinkedHashMap<>();
    item.put("name", "solo");
    final List<Map<String, Object>> people = new ArrayList<>(List.of(item));

    final Map<String, Object> params = new HashMap<>();
    params.put("people", people);

    final ResultSet result = database.command("sql", "INSERT INTO " + typeName + " CONTENT :people", params);

    assertThat(result.hasNext()).isTrue();
    final Result inserted = result.next();
    assertThat(inserted.<String>getProperty("name")).isEqualTo("solo");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void insertContentWithPositionalListParamCreatesOneRecordPerItem() {
    final String typeName = "Issue6463PositionalPerson";
    database.getSchema().createDocumentType(typeName);

    final List<Map<String, Object>> people = new ArrayList<>();
    for (final String name : List.of("x", "y", "z")) {
      final Map<String, Object> item = new LinkedHashMap<>();
      item.put("name", name);
      people.add(item);
    }

    final ResultSet result = database.command("sql", "INSERT INTO " + typeName + " CONTENT ?", (Object) people);

    final List<String> insertedNames = new ArrayList<>();
    while (result.hasNext())
      insertedNames.add(result.next().<String>getProperty("name"));
    result.close();

    assertThat(insertedNames).containsExactlyInAnyOrder("x", "y", "z");

    final ResultSet count = database.query("sql", "SELECT count(*) as total FROM " + typeName);
    assertThat(count.hasNext()).isTrue();
    assertThat(count.next().<Long>getProperty("total")).isEqualTo(3L);
    count.close();
  }

  @Test
  void insertContentWithSingleMapParamStillCreatesOneRecord() {
    final String typeName = "Issue6463MapParam";
    database.getSchema().createDocumentType(typeName);

    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("name", "mapParam");

    final Map<String, Object> params = new HashMap<>();
    params.put("content", content);

    final ResultSet result = database.command("sql", "INSERT INTO " + typeName + " CONTENT :content", params);

    assertThat(result.hasNext()).isTrue();
    final Result inserted = result.next();
    assertThat(inserted.<String>getProperty("name")).isEqualTo("mapParam");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  /**
   * An empty-list {@code CONTENT :param} deliberately keeps the pre-existing {@code tot = 1} sizing of the
   * equivalent empty JSON-array literal ({@code CONTENT []}), rather than creating zero records. This pins that
   * documented edge case down (see the PR description for #6463).
   */
  @Test
  void insertContentWithEmptyListParamStillCreatesOneRecord() {
    final String typeName = "Issue6463EmptyListParam";
    database.getSchema().createDocumentType(typeName);

    final Map<String, Object> params = new HashMap<>();
    params.put("people", new ArrayList<Map<String, Object>>());

    final ResultSet result = database.command("sql", "INSERT INTO " + typeName + " CONTENT :people", params);

    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    final ResultSet count = database.query("sql", "SELECT count(*) as total FROM " + typeName);
    assertThat(count.hasNext()).isTrue();
    assertThat(count.next().<Long>getProperty("total")).isEqualTo(1L);
    count.close();
  }

  /**
   * {@code CreateVertexExecutionPlanner} extends {@link InsertExecutionPlanner} and reuses its
   * {@code handleCreateRecord()} unchanged, so {@code CREATE VERTEX ... CONTENT :param} with a {@link List}-valued
   * parameter must get the same one-record-per-item fix as plain {@code INSERT}.
   */
  @Test
  void createVertexContentWithListParamCreatesOneVertexPerItem() {
    final String typeName = "Issue6463Vertex";
    database.getSchema().createVertexType(typeName);

    final List<Map<String, Object>> people = new ArrayList<>();
    for (final String name : List.of("v1", "v2", "v3")) {
      final Map<String, Object> item = new LinkedHashMap<>();
      item.put("name", name);
      people.add(item);
    }

    final Map<String, Object> params = new HashMap<>();
    params.put("people", people);

    final ResultSet result = database.command("sql", "CREATE VERTEX " + typeName + " CONTENT :people", params);

    final List<String> insertedNames = new ArrayList<>();
    while (result.hasNext())
      insertedNames.add(result.next().<String>getProperty("name"));
    result.close();

    assertThat(insertedNames).containsExactlyInAnyOrder("v1", "v2", "v3");

    final ResultSet count = database.query("sql", "SELECT count(*) as total FROM " + typeName);
    assertThat(count.hasNext()).isTrue();
    assertThat(count.next().<Long>getProperty("total")).isEqualTo(3L);
    count.close();
  }
}
