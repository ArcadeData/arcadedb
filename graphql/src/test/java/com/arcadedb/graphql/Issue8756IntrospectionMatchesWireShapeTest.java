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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8756: the introspection descriptor of a database-only type's property must name what the GraphQL result actually puts on the
 * wire. Introspects the type over HTTP, queries one record of it over HTTP, and checks every property's JSON value against its
 * descriptor - so a change on either side (the descriptor, or the serialization of a property type) turns this red.
 */
class Issue8756IntrospectionMatchesWireShapeTest extends BaseGraphServerTest {

  private static final String[] PROPERTIES = { "attributes", "home", "anything", "owner", "anyLink", "price", "payload", "created",
      "createdMicros", "createdNanos", "createdSecond", "birthday", "opensAt", "closesAt", "zoned", "ttl", "links", "maps", "prices",
      "blobs", "embeddeds" };

  @Test
  void everyDescriptorMatchesTheSerializedValue() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      final DocumentType address = database.getSchema().createDocumentType("Address");
      address.createProperty("city", Type.STRING);
      final DocumentType doc = database.getSchema().createDocumentType("Doc");
      doc.createProperty("attributes", Type.MAP);
      doc.createProperty("home", Type.EMBEDDED, "Address");
      doc.createProperty("anything", Type.EMBEDDED);
      doc.createProperty("owner", Type.LINK, "Address");
      doc.createProperty("anyLink", Type.LINK);
      doc.createProperty("price", Type.DECIMAL);
      doc.createProperty("payload", Type.BINARY);
      doc.createProperty("created", Type.DATETIME);
      doc.createProperty("createdMicros", Type.DATETIME_MICROS);
      doc.createProperty("createdNanos", Type.DATETIME_NANOS);
      doc.createProperty("createdSecond", Type.DATETIME_SECOND);
      doc.createProperty("birthday", Type.DATE);
      doc.createProperty("opensAt", Type.LOCAL_TIME);
      doc.createProperty("closesAt", Type.OFFSET_TIME);
      doc.createProperty("zoned", Type.ZONED_DATETIME);
      doc.createProperty("ttl", Type.DURATION);
      doc.createProperty("links", Type.LIST, "LINK");
      doc.createProperty("maps", Type.LIST, "MAP");
      doc.createProperty("prices", Type.LIST, "DECIMAL");
      doc.createProperty("blobs", Type.LIST, "BINARY");
      doc.createProperty("embeddeds", Type.LIST, "EMBEDDED");
    });

    database.transaction(() -> {
      final MutableDocument rome = database.newDocument("Address").set("city", "Rome");
      rome.save();
      final RID romeRid = rome.getIdentity();

      final Map<String, Object> attributes = new HashMap<>();
      attributes.put("color", "red");
      attributes.put("size", 3);

      final MutableDocument doc = database.newDocument("Doc");
      doc.set("attributes", attributes);
      doc.newEmbeddedDocument("Address", "home").set("city", "Milan");
      doc.newEmbeddedDocument("Address", "anything").set("city", "Turin");
      doc.set("owner", romeRid);
      doc.set("anyLink", romeRid);
      doc.set("price", new BigDecimal("12.345678901234567890123"));
      doc.set("payload", new byte[] { 1, 2, -3 });
      doc.set("created", LocalDateTime.of(2026, 10, 7, 10, 15, 30));
      doc.set("createdMicros", LocalDateTime.of(2026, 10, 7, 10, 15, 30, 123_456_000));
      doc.set("createdNanos", LocalDateTime.of(2026, 10, 7, 10, 15, 30, 123_456_789));
      doc.set("createdSecond", LocalDateTime.of(2026, 10, 7, 10, 15, 30));
      doc.set("birthday", LocalDate.of(2000, 1, 2));
      doc.set("opensAt", LocalTime.of(9, 0));
      doc.set("closesAt", OffsetTime.of(18, 0, 0, 0, ZoneOffset.ofHours(1)));
      doc.set("zoned", ZonedDateTime.of(2026, 1, 1, 10, 15, 30, 0, ZoneOffset.ofHours(1)));
      doc.set("ttl", CypherDuration.parse("PT1M30.5S"));
      doc.set("links", List.of(romeRid));
      doc.set("maps", List.of(Map.of("k", 1)));
      doc.set("prices", List.of(new BigDecimal("1.5")));
      doc.set("blobs", List.of(new byte[] { 4, 5 }));
      final MutableEmbeddedDocument pisa = doc.newEmbeddedDocument("Address", "scratch");
      pisa.set("city", "Pisa");
      doc.set("embeddeds", List.of(pisa));
      doc.remove("scratch");
      doc.save();
    });

    final JSONArray fields = post("query",
        "{ __type(name: \"Doc\") { fields { name type { kind name ofType { kind name ofType { kind name } } } } } }")
        .getJSONArray("result").getJSONObject(0).getJSONArray("fields");

    // Doc is database-only: a Query is needed to reach its records, so the SDL declares a view type with none of its properties
    post("command", "type Query { docs: [DocView] @sql(statement: \"SELECT FROM Doc\") } type DocView { id: String }");
    final JSONObject record = post("query", "{ docs { " + String.join(" ", PROPERTIES) + " } }").getJSONArray("result")
        .getJSONObject(0);

    for (final String property : PROPERTIES) {
      final JSONObject descriptor = descriptorOf(fields, property);
      assertThat(record.has(property)).as("property %s is serialized", property).isTrue();
      assertMatches(property, descriptor, record.get(property));
    }

    // An EMBEDDED property described as an OBJECT must also be walkable with a sub-selection, as an OBJECT is
    final JSONObject walked = post("query", "{ docs { home { city } } }").getJSONArray("result").getJSONObject(0);
    assertThat(walked.getJSONObject("home").getString("city")).isEqualTo("Milan");
  }

  /**
   * The JSON value a field with this descriptor carries: a LIST is an array of values matching its element, an OBJECT or the JSON
   * scalar an object, ID and String a string, BigDecimal / Int / Float a number.
   */
  private static void assertMatches(final String property, final JSONObject descriptor, final Object value) {
    final String kind = descriptor.getString("kind");
    switch (kind) {
    case "LIST" -> {
      assertThat(value).as("%s is a LIST", property).isInstanceOf(JSONArray.class);
      final JSONArray array = (JSONArray) value;
      assertThat(array.length()).as("%s has an element to check", property).isPositive();
      for (int i = 0; i < array.length(); i++)
        assertMatches(property + "[" + i + "]", descriptor.getJSONObject("ofType"), array.get(i));
    }
    case "OBJECT" -> assertThat(value).as("%s is an OBJECT", property).isInstanceOf(JSONObject.class);
    case "SCALAR" -> {
      final String name = descriptor.getString("name");
      switch (name) {
      case "JSON" -> assertThat(value).as("%s is a JSON object", property).isInstanceOf(JSONObject.class);
      case "ID", "String" -> assertThat(value).as("%s is a %s", property, name).isInstanceOf(String.class);
      case "BigDecimal", "Int", "Float", "Long" -> assertThat(value).as("%s is a %s", property, name).isInstanceOf(Number.class);
      default -> throw new AssertionError(property + " is described with an unexpected scalar " + name);
      }
    }
    default -> throw new AssertionError(property + " is described with an unexpected kind " + kind);
    }
  }

  private static JSONObject descriptorOf(final JSONArray fields, final String name) {
    for (int i = 0; i < fields.length(); i++)
      if (name.equals(fields.getJSONObject(i).getString("name")))
        return fields.getJSONObject(i).getJSONObject("type");
    throw new AssertionError("field not found: " + name);
  }

  private JSONObject post(final String endpoint, final String text) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        getServerHttpUrl(0, "/api/v1/" + endpoint + "/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream()
        .write(new JSONObject().put("language", "graphql").put("command", text).toString().getBytes(StandardCharsets.UTF_8));
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(readResponse(connection));
    } finally {
      connection.disconnect();
    }
  }
}
