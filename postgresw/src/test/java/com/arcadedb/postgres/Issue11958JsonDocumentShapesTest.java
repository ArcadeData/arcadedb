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
package com.arcadedb.postgres;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11958: every place a json column renders a record, a row, a map or a path (the scalar column, an element of a json
 * column holding an array, an element of a text[] or json[] array in text and in binary format) writes its temporals as the
 * HTTP API does. A path and a row keep the shape {@code path.toMap()} and {@code result.toJSON()} gave them: the same members,
 * the same non-temporal values, and a node or a relationship of a path gains the record attributes the HTTP API sends with it.
 */
class Issue11958JsonDocumentShapesTest extends TestHelper {

  private static final Set<String> TEMPORALS = Set.of("da", "dt", "dtn", "zdt");

  @BeforeEach
  void createGraph() {
    database.command("sql", "CREATE VERTEX TYPE T11958");
    database.command("sql", "CREATE PROPERTY T11958.da DATE");
    database.command("sql", "CREATE PROPERTY T11958.dt DATETIME");
    database.command("sql", "CREATE PROPERTY T11958.dtn DATETIME_NANOS");
    database.command("sql", "CREATE PROPERTY T11958.zdt ZONED_DATETIME");
    database.command("sql", "CREATE EDGE TYPE E11958");
    database.command("sql", "CREATE PROPERTY E11958.da DATE");
    database.transaction(() -> {
      for (final String name : new String[] { "a", "b" })
        database.command("sql", "INSERT INTO T11958 SET name = ?, n = 7, da = '2024-02-29', dt = '2024-02-29 13:14:15.123', "
            + "dtn = '2024-02-29 13:14:15.123456789', zdt = '2024-03-10T03:30-04:00[America/New_York]'", name);
      database.command("sql", "CREATE EDGE E11958 FROM (SELECT FROM T11958 WHERE name = 'a') TO (SELECT FROM T11958 WHERE name = 'b') "
          + "SET da = '2024-02-29', w = 2");
    });
  }

  @Test
  void aJsonColumnHoldingAListOfRecordsMapsAndPaths() {
    final List<Object> values = List.of(vertex(), map(), List.of(map()), path());
    assertThat(PostgresType.getTypeForValue(values)).isEqualTo(PostgresType.JSON);

    final JSONArray array = new JSONArray(PostgresType.JSON.toText(PostgresType.JSON, values));
    assertTemporals(array.getJSONObject(0));
    assertTemporals(array.getJSONObject(1));
    assertTemporals(array.getJSONArray(2).getJSONObject(0));
    final JSONObject path = array.getJSONObject(3);
    assertThat(path.getString("_type")).isEqualTo("path");
    assertTemporals(path.getJSONArray("nodes").getJSONObject(0));
    assertThat(path.getJSONArray("relationships").getJSONObject(0).get("da")).isEqualTo("2024-02-29");
  }

  @Test
  void aJsonDocumentNestedInsideAnArray() {
    final List<Object> values = List.of("text", map(), vertex(), path());
    assertThat(PostgresType.getTypeForValue(values)).isEqualTo(PostgresType.ARRAY_TEXT);

    // text format: the Postgres array literal, each document a quoted element
    final List<Object> literal = PostgresArrayLiteral.parse(PostgresType.ARRAY_TEXT.toText(PostgresType.ARRAY_TEXT, values), s -> s);
    assertElements(literal);

    // binary format, as text[] and as json[]
    for (final PostgresType arrayType : new PostgresType[] { PostgresType.ARRAY_TEXT, PostgresType.ARRAY_JSON }) {
      final Binary buffer = new Binary();
      arrayType.serializeAsBinary(arrayType, buffer, values);
      buffer.flip();
      final byte[] data = new byte[buffer.getInt()];
      buffer.getByteBuffer().get(data);
      assertElements((List<?>) PostgresType.deserialize(arrayType.code, 1, data));
    }
  }

  @Test
  void aPathKeepsTheShapeOfItsMapAndGainsTheRecordAttributes() {
    final TraversalPath path = path();
    // read back from its text, as the client reads the column
    final JSONObject before = new JSONObject(new JSONObject(path.toMap()).toString());
    final JSONObject after = new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, path));

    assertThat(after.keySet()).containsExactlyElementsOf(before.keySet());
    assertThat(after.getString("_type")).isEqualTo("path");
    assertThat(after.getInt("length")).isEqualTo(before.getInt("length")).isEqualTo(1);
    assertThat(after.getJSONArray("nodes").length()).isEqualTo(before.getJSONArray("nodes").length()).isEqualTo(2);
    for (int i = 0; i < 2; i++)
      assertSameShape(before.getJSONArray("nodes").getJSONObject(i), after.getJSONArray("nodes").getJSONObject(i),
          Set.of("@rid", "@type", "@cat"));
    assertSameShape(before.getJSONArray("relationships").getJSONObject(0), after.getJSONArray("relationships").getJSONObject(0),
        Set.of("@rid", "@type", "@cat", "@in", "@out"));
  }

  @Test
  void aRowThatIsNotAnElementKeepsTheShapeOfItsJson() {
    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("name", "a");
    content.put("n", 7);
    content.put("da", LocalDate.of(2024, 2, 29));
    content.put("dt", LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_000_000));
    content.put("dtn", LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_789));
    content.put("nested", new LinkedHashMap<>(Map.of("da", LocalDate.of(2024, 2, 29))));
    final ResultInternal row = new ResultInternal(content);
    assertThat(row.isElement()).isFalse();
    assertThat(row.getDatabase()).isNull();

    final JSONObject before = new JSONObject(row.toJSON().toString());
    final JSONObject after = new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, row));
    assertThat(after.keySet()).containsExactlyElementsOf(before.keySet());
    assertThat(after.get("name")).isEqualTo(before.get("name")).isEqualTo("a");
    assertThat(after.get("n")).isEqualTo(before.get("n")).isEqualTo(7);
    assertThat(after.get("da")).isEqualTo("2024-02-29");
    assertThat(after.get("dt")).isEqualTo("2024-02-29 13:14:15.123");
    assertThat(after.get("dtn")).isEqualTo("2024-02-29 13:14:15.123456789");
    assertThat(after.getJSONObject("nested").get("da")).isEqualTo("2024-02-29");
  }

  @Test
  void aRowThatIsNotAnElementUsesTheDatePatternOfItsDatabase() {
    database.getSchema().setDateFormat("dd/MM/yyyy");
    try (final ResultSet rs = database.query("sql", "SELECT da AS d, name FROM T11958 WHERE name = 'a'")) {
      final Result row = rs.next();
      assertThat(row.isElement()).isFalse();
      assertThat(row.getDatabase()).as("a projection row knows its database").isNotNull();
      assertThat(new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, row)).get("d")).isEqualTo("29/02/2024");
    }
    // the same value in a row no database owns takes the configured default pattern
    final ResultInternal detached = new ResultInternal(new LinkedHashMap<>(Map.of("d", LocalDate.of(2024, 2, 29))));
    assertThat(new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, detached)).get("d")).isEqualTo("2024-02-29");
  }

  @Test
  void aRecordThatIsNotADocumentKeepsItsOwnJson() {
    final MutableEdgeSegment segment = new MutableEdgeSegment((DatabaseInternal) database, 64);
    segment.setIdentity(new RID(5, 0));
    assertThat(PostgresType.getTypeForValue(segment)).isEqualTo(PostgresType.JSON);

    final String text = PostgresType.JSON.toText(PostgresType.JSON, segment);
    assertThat(text).isEqualTo(segment.toJSON(true).toString());
    assertThat(new JSONObject(text).getString("@rid")).isEqualTo("#5:0");
  }

  /**
   * A detached copy has no database. It is the value the engine hands out of a transaction (the before-image of a DELETE on a type
   * with EXTERNAL properties), and a json column used to render it through {@code toJSON(true)}: it must not start to fail for want of
   * the database the HTTP serializer reads the date patterns from.
   */
  @Test
  void aDetachedRecordKeepsItsTemporalText() {
    final DetachedDocument detached = vertex().detach();
    assertThat(detached.getDatabase()).isNull();

    assertTemporals(new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, detached)));
    // a row that wraps it (the before-image step of a DELETE answers one) has no database to ask either
    assertTemporals(new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, new ResultInternal(detached))));
    assertTemporals(new JSONArray(PostgresType.JSON.toText(PostgresType.JSON, List.of(detached))).getJSONObject(0));
    assertTemporals(new JSONObject(PostgresType.JSON.toText(PostgresType.JSON, Map.of("nested", detached))).getJSONObject("nested"));
  }

  /**
   * A json document inside a list that is itself an element of a text[] or json[] array (a nested list or an array of the element),
   * in the literal and in both binary forms, carries the same temporal text as one directly in the column.
   */
  @Test
  void aJsonDocumentInsideAListNestedInAnArray() {
    final List<Object> values = List.of("text", List.of(map(), List.of(map())), new Object[] { map() });
    assertThat(PostgresType.getTypeForValue(values)).isEqualTo(PostgresType.ARRAY_TEXT);

    assertNestedElements(PostgresArrayLiteral.parse(PostgresType.ARRAY_TEXT.toText(PostgresType.ARRAY_TEXT, values), s -> s));

    for (final PostgresType arrayType : new PostgresType[] { PostgresType.ARRAY_TEXT, PostgresType.ARRAY_JSON }) {
      final Binary buffer = new Binary();
      arrayType.serializeAsBinary(arrayType, buffer, values);
      buffer.flip();
      final byte[] data = new byte[buffer.getInt()];
      buffer.getByteBuffer().get(data);
      assertNestedElements((List<?>) PostgresType.deserialize(arrayType.code, 1, data));
    }
  }

  private static void assertNestedElements(final List<?> elements) {
    assertThat(elements).hasSize(3);
    assertThat(elements.get(0)).isEqualTo("text");
    final JSONArray nested = new JSONArray((String) elements.get(1));
    assertTemporals(nested.getJSONObject(0));
    assertTemporals(nested.getJSONArray(1).getJSONObject(0));
    assertTemporals(new JSONArray((String) elements.get(2)).getJSONObject(0));
  }

  private static void assertElements(final List<?> elements) {
    assertThat(elements).hasSize(4);
    assertThat(elements.get(0)).isEqualTo("text");
    assertTemporals(new JSONObject((String) elements.get(1)));
    assertTemporals(new JSONObject((String) elements.get(2)));
    final JSONObject path = new JSONObject((String) elements.get(3));
    assertThat(path.getString("_type")).isEqualTo("path");
    assertTemporals(path.getJSONArray("nodes").getJSONObject(0));
  }

  /**
   * {@code after} has every member {@code before} has, plus {@code added}; a non-temporal member keeps its value, and a temporal one
   * that was an epoch number is now the text the HTTP API sends.
   */
  private static void assertSameShape(final JSONObject before, final JSONObject after, final Set<String> added) {
    final Set<String> expected = new HashSet<>(before.keySet());
    expected.addAll(added);
    assertThat(after.keySet()).containsExactlyInAnyOrderElementsOf(expected);
    for (final String name : before.keySet())
      if (TEMPORALS.contains(name))
        assertThat(after.get(name)).as(name).isInstanceOf(String.class);
      else
        assertThat(after.get(name)).as(name).isEqualTo(before.get(name));
    if (after.has("dtn"))
      assertTemporals(after);
  }

  private Vertex vertex() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM T11958 WHERE name = 'a'")) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  private Map<String, Object> map() {
    try (final ResultSet rs = database.query("sql", "SELECT {'da': da, 'dt': dt, 'dtn': dtn, 'zdt': zdt} AS m FROM T11958 WHERE name = 'a'")) {
      return rs.next().getProperty("m");
    }
  }

  private TraversalPath path() {
    try (final ResultSet rs = database.query("opencypher", "MATCH p = (:T11958 {name: 'a'})-[:E11958]->(:T11958) RETURN p")) {
      return rs.next().getProperty("p");
    }
  }

  private static void assertTemporals(final JSONObject json) {
    assertThat(json.get("da")).isEqualTo("2024-02-29");
    assertThat(json.get("dt")).isEqualTo("2024-02-29 13:14:15.123");
    assertThat(json.get("dtn")).isEqualTo("2024-02-29 13:14:15.123456789");
    assertThat(json.get("zdt")).isEqualTo("2024-03-10T03:30-04:00[America/New_York]");
  }
}
