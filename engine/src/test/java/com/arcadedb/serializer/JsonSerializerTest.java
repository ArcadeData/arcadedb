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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class JsonSerializerTest extends TestHelper {

  private JsonSerializer jsonSerializer;

  @BeforeEach
  void setUp() {
    jsonSerializer = JsonSerializer.createJsonSerializer();
  }

  @Test
  void serializeDocument() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createDocumentType("TestType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableDocument document = database.newDocument("TestType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(document).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void serializeVertex() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createVertexType("TestVertexType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableVertex vertex = database.newVertex("TestVertexType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(vertex).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestVertexType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void serializeEdge() {
    database.transaction(() -> {
      DocumentType vertexType = database.getSchema().createVertexType("TestVertexType");
      DocumentType edgeType = database.getSchema().createEdgeType("TestEdgeType");

      MutableVertex vertex1 = database.newVertex("TestVertexType").save();
      MutableVertex vertex2 = database.newVertex("TestVertexType").save();

      MutableEdge edge = vertex1.newEdge("TestEdgeType", vertex2).save();

      String json = jsonSerializer.serializeDocument(edge).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestEdgeType");
      assertThat(JsonPath.<String>read(json, "$.@in")).isEqualTo(vertex2.getIdentity().toString());
      assertThat(JsonPath.<String>read(json, "$.@out")).isEqualTo(vertex1.getIdentity().toString());
    });
  }

  @Test
  void expandScalarListDoesNotEmitPropsHint() {
    try (final ResultSet rs = database.query("sql", "SELECT expand([1,2,3,4]) AS test")) {
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final JSONObject json = jsonSerializer.serializeResult(database, row);
        assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isFalse();
        assertThat(json.has("test")).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(4);
    }
  }

  @Test
  void stringExpandDoesNotEmitPropsHint() {
    try (final ResultSet rs = database.query("sql", "SELECT expand(['a','b','c']) AS tag")) {
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final JSONObject json = jsonSerializer.serializeResult(database, row);
        assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isFalse();
        assertThat(json.has("tag")).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(3);
    }
  }

  @Test
  void scalarProjectionDoesNotEmitPropsHintForJsonFaithfulTypes() {
    try (final ResultSet rs = database.query("sql", "SELECT 1 AS i, 'x' AS s, true AS b")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isFalse();
      assertThat(json.getInt("i")).isEqualTo(1);
      assertThat(json.getString("s")).isEqualTo("x");
      assertThat(json.getBoolean("b")).isTrue();
    }
  }

  @Test
  void doublePropertyDoesNotEmitPropsHint() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("DoubleType");
      type.createProperty("d", Type.DOUBLE);
      database.newDocument("DoubleType").set("d", 2.5d).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT d FROM DoubleType")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isFalse();
    }
  }

  @Test
  void floatProjectionStillEmitsPropsHint() {
    // 2.5 is inferred as FLOAT by the SQL parser (not DOUBLE); FLOAT is lossy through JSON (deserializes to Double), so the hint must stay.
    try (final ResultSet rs = database.query("sql", "SELECT 2.5 AS f")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isTrue();
      assertThat(json.getString(Property.PROPERTY_TYPES_PROPERTY)).contains("f:" + Type.FLOAT.getId());
    }
  }

  @Test
  void floatSchemaPropertyStillEmitsPropsHint() {
    // Schema-backed FLOAT, independent of how the SQL parser types a decimal literal.
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("FloatType");
      type.createProperty("f", Type.FLOAT);
      database.newDocument("FloatType").set("f", 2.5f).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT f FROM FloatType")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isTrue();
      assertThat(json.getString(Property.PROPERTY_TYPES_PROPERTY)).contains("f:" + Type.FLOAT.getId());
    }
  }

  @Test
  void mixedFaithfulAndLossyColumnsEmitHintOnlyForLossy() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("MixedType");
      for (int i = 0; i < 2; i++)
        database.newDocument("MixedType").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT 1 AS i, count(*) AS c FROM MixedType")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      // i is an Integer (JSON-faithful) so it is absent from the hint; c is a Long (lossy) so it stays.
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isTrue();
      final String hint = json.getString(Property.PROPERTY_TYPES_PROPERTY);
      assertThat(hint).contains("c:" + Type.LONG.getId());
      assertThat(hint).doesNotContain("i:");
    }
  }

  @Test
  void countStarStillEmitsPropsHintForLong() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("CountType");
      for (int i = 0; i < 3; i++)
        database.newDocument("CountType").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM CountType")) {
      assertThat(rs.hasNext()).isTrue();
      final JSONObject json = jsonSerializer.serializeResult(database, rs.next());
      // count(*) yields a Long, which JSON cannot round-trip (collapses to Integer), so the hint stays.
      assertThat(json.has(Property.PROPERTY_TYPES_PROPERTY)).isTrue();
      assertThat(json.getString(Property.PROPERTY_TYPES_PROPERTY)).contains("c:" + Type.LONG.getId());
    }
  }

  /**
   * Issue #4601: a DATE property must be serialized to JSON as epoch DAYS (the encoding the server
   * decodes), not epoch milliseconds. Otherwise a java.util.Date written through the remote client is
   * read back by the server as a huge out-of-range day count and silently dropped to null.
   */
  @Test
  void dateTypeSerializedAsEpochDays() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("DateJsonType");
      type.createProperty("d", Type.DATE);
    });

    final LocalDate localDate = LocalDate.of(2026, 6, 12);
    final long expectedDays = localDate.toEpochDay();
    final Date javaDate = Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());

    database.transaction(() -> {
      // A java.util.Date assigned to a DATE property (mirrors the remote client path).
      final MutableDocument doc = database.newDocument("DateJsonType").set("d", javaDate);
      assertThat(doc.toJSON(false).getLong("d")).isEqualTo(expectedDays);

      // A LocalDate assigned to a DATE property must encode the same way.
      final MutableDocument doc2 = database.newDocument("DateJsonType").set("d", localDate);
      assertThat(doc2.toJSON(false).getLong("d")).isEqualTo(expectedDays);
    });
  }
}
