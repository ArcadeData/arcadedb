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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8332: building a {@link ResultSet} out of a {@code /query} response converted every projection row from the
 * parsed JSON tree into a {@code LinkedHashMap} twice - once in {@code json2Record()} only to learn the row carries no
 * {@code @cat}, then again in {@code json2Result()} - and parsed the row's {@code @props} hint again for every row,
 * although every row of a projection carries the identical string.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8332RemoteResultConversionTest {
  private TestableRemoteDatabase database;

  static class TestableRemoteDatabase extends RemoteDatabase {
    int json2RecordCalls;

    TestableRemoteDatabase() {
      super("localhost", 2480, "testdb", "root", "test", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // NO HTTP CALLS IN A UNIT TEST
    }

    @Override
    protected Record json2Record(final JSONObject result) {
      ++json2RecordCalls;
      return super.json2Record(result);
    }

    ResultSet build(final JSONObject response) {
      return createResultSet(response);
    }
  }

  /** Counts how many times the row is converted to a Map: the conversion the issue measured twice per row. */
  static class CountingJSONObject extends JSONObject {
    int toMapCalls;

    CountingJSONObject(final String json) {
      super(json);
    }

    @Override
    public Map<String, Object> toMap(final boolean optimizeNumericArrays) {
      ++toMapCalls;
      return super.toMap(optimizeNumericArrays);
    }
  }

  @BeforeEach
  void setUp() {
    database = new TestableRemoteDatabase();
  }

  @AfterEach
  void tearDown() {
    if (database.isOpen())
      database.close();
  }

  @Test
  void projectionRowIsConvertedToAMapOnce() {
    final CountingJSONObject row = new CountingJSONObject("{\"k\":7,\"name\":\"seven\",\"x\":1.5,\"@props\":\"k:3\"}");

    final Result result = database.json2Result(row);

    assertThat(row.toMapCalls).isEqualTo(1);
    // No @cat on the row: there is no record to build, so the record path is not even entered.
    assertThat(database.json2RecordCalls).isZero();

    assertThat(result.isElement()).isFalse();
    assertThat(result.<Object>getProperty("k")).isInstanceOf(Long.class).isEqualTo(7L);
    assertThat(result.<String>getProperty("name")).isEqualTo("seven");
    assertThat(result.<Double>getProperty("x")).isEqualTo(1.5);
    // The hint is metadata, not a column, and the column order is the server's.
    assertThat(result.getPropertyNames()).containsExactly("k", "name", "x");
  }

  @Test
  void projectionRowWithoutHintsKeepsEveryField() {
    final CountingJSONObject row = new CountingJSONObject("{\"@rid\":\"#3:4\",\"k\":7}");

    final Result result = database.json2Result(row);

    assertThat(row.toMapCalls).isEqualTo(1);
    // Without @props the row is handed over untouched, as before: nothing to strip, nothing to convert.
    assertThat(result.getPropertyNames()).containsExactly("@rid", "k");
    assertThat(result.<Integer>getProperty("k")).isEqualTo(7);
  }

  @Test
  void rowWithUnknownCategoryFallsBackToAProjectionConvertedOnce() {
    final CountingJSONObject row = new CountingJSONObject("{\"@cat\":\"x\",\"a\":1,\"@props\":\"a:3\"}");

    final Result result = database.json2Result(row);

    // json2Record() is asked, finds no category it can build, and must not have paid a conversion for that answer.
    assertThat(database.json2RecordCalls).isEqualTo(1);
    assertThat(row.toMapCalls).isEqualTo(1);
    assertThat(result.isElement()).isFalse();
    assertThat(result.getPropertyNames()).containsExactly("a");
    assertThat(result.<Object>getProperty("a")).isEqualTo(1L);
  }

  @Test
  void hintedRowDropsEveryMetadataField() {
    final Result result = database.json2Result(
        new JSONObject("{\"@rid\":\"#3:4\",\"@type\":\"T\",\"@in\":\"#1:1\",\"@out\":\"#1:2\",\"n\":1,\"@props\":\"n:3\"}"));

    assertThat(result.getPropertyNames()).containsExactly("n");
    assertThat(result.<Object>getProperty("n")).isEqualTo(1L);
  }

  @Test
  void everyRowOfAResultSetIsConvertedOnceAndTyped() {
    final JSONArray rows = new JSONArray();
    for (int i = 0; i < 100; i++)
      rows.put(new JSONObject().put("k", i).put("name", "n" + i).put(Property.PROPERTY_TYPES_PROPERTY, "k:3"));
    final JSONObject response = new JSONObject().put("result", rows);

    long sum = 0;
    int count = 0;
    try (final ResultSet resultSet = database.build(response)) {
      while (resultSet.hasNext()) {
        final Result r = resultSet.next();
        assertThat(r.<Object>getProperty("k")).isInstanceOf(Long.class);
        assertThat(r.hasProperty(Property.PROPERTY_TYPES_PROPERTY)).isFalse();
        sum += r.<Long>getProperty("k");
        ++count;
      }
    }
    assertThat(count).isEqualTo(100);
    assertThat(sum).isEqualTo(4950L);
    assertThat(database.json2RecordCalls).isZero();
  }

  @Test
  void identicalHintStringIsParsedOnce() {
    // Two distinct String instances with the same content, as two rows parsed off the wire would carry.
    final String first = new String("k:3,d:9(6)");
    final String second = new String("k:3,d:9(6)");

    final Map<String, ?> a = database.propertyTypeHints(first);
    final Map<String, ?> b = database.propertyTypeHints(second);
    assertThat(b).isSameAs(a);
    assertThat(a).containsOnlyKeys("k", "d");

    // A row whose shape differs is parsed on its own, never answered with the previous row's hints.
    final Map<String, ?> c = database.propertyTypeHints("k:1");
    assertThat(c).isNotSameAs(a).containsOnlyKeys("k");
    assertThat(database.propertyTypeHints(null)).isEmpty();
    assertThat(database.propertyTypeHints("")).isEmpty();
  }

  @Test
  void hintedRowsOfDifferentShapesEachKeepTheirOwnTypes() {
    final Result first = database.json2Result(new JSONObject("{\"a\":1,\"@props\":\"a:3\"}"));
    final Result second = database.json2Result(new JSONObject("{\"a\":1,\"@props\":\"a:1\"}"));
    final Result third = database.json2Result(new JSONObject("{\"a\":1,\"@props\":\"a:3\"}"));

    assertThat(first.<Object>getProperty("a")).isEqualTo(1L);
    assertThat(second.<Object>getProperty("a")).isEqualTo(1);
    assertThat(third.<Object>getProperty("a")).isEqualTo(1L);
  }

  /**
   * Found while fixing #8332: {@code RemoteImmutableDocument} had its own {@code @props} parser that called
   * {@code put()} on a {@code null} map, so every hint of a record carrying one threw a NullPointerException, was
   * logged as SEVERE and dropped - and it split on ':' so an element-type suffix such as {@code 9(6)} never parsed.
   */
  @Test
  void recordHonorsItsPropertyTypeHints() {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestDoc");
    attributes.put(Property.CAT_PROPERTY, "d");
    attributes.put(Property.RID_PROPERTY, "#1:0");
    attributes.put("n", 5);
    attributes.put("tags", List.of("a", "b"));
    attributes.put(Property.PROPERTY_TYPES_PROPERTY, "n:3,tags:9(7)");

    final RemoteImmutableDocument doc = new RemoteImmutableDocument(schemalessDatabase(), attributes);

    assertThat(doc.get("n")).isInstanceOf(Long.class).isEqualTo(5L);
    assertThat(doc.get("tags")).isEqualTo(List.of("a", "b"));
    assertThat(doc.getPropertyNames()).containsExactlyInAnyOrder("n", "tags");
  }

  /**
   * A schemaless collection field has only its hint to go by: the element type it names must reach every item, even
   * though the list or map the JSON parser built is already assignable to the column type.
   */
  @Test
  @SuppressWarnings("unchecked")
  void recordHonorsTheElementTypeOfASchemalessCollectionHint() {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.put(Property.TYPE_PROPERTY, "TestDoc");
    attributes.put(Property.CAT_PROPERTY, "d");
    attributes.put(Property.RID_PROPERTY, "#1:0");
    attributes.put("nums", List.of(1, 2));
    attributes.put("byKey", Map.of("a", 3));
    attributes.put("plain", List.of(4));
    attributes.put(Property.PROPERTY_TYPES_PROPERTY, "nums:9(3),byKey:10(3),plain:9");

    final RemoteImmutableDocument doc = new RemoteImmutableDocument(schemalessDatabase(), attributes);

    assertThat((List<Object>) doc.get("nums")).containsExactly(1L, 2L);
    assertThat((Map<Object, Object>) doc.get("byKey")).containsEntry("a", 3L);
    // No element type in the hint: the items are left as parsed.
    assertThat((List<Object>) doc.get("plain")).containsExactly(4);
  }

  private static RemoteDatabase schemalessDatabase() {
    final RemoteDatabase mockDatabase = mock(RemoteDatabase.class);
    final RemoteSchema mockSchema = mock(RemoteSchema.class);
    final RemoteDocumentType mockType = mock(RemoteDocumentType.class);
    when(mockDatabase.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getType("TestDoc")).thenReturn(mockType);
    when(mockType.getName()).thenReturn("TestDoc");
    when(mockType.getPolymorphicPropertyIfExists(ArgumentMatchers.anyString())).thenReturn(null);
    when(mockDatabase.newRID(ArgumentMatchers.anyString())).thenAnswer(inv -> new RID(inv.getArgument(0)));
    return mockDatabase;
  }
}
