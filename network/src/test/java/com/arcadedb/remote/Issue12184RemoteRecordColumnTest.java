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
import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12184: a record held by a column of a projection row (Cypher {@code RETURN a, r, b}, SQL {@code @this},
 * {@code first(out())}, {@code collect(a)}) reached the remote client as a list of RIDs: the server writes the column as the
 * record's JSON next to a {@code LINK} hint in {@code @props}, and the hint converted the JSON map to an Identifiable, which kept
 * the strings that parse as RIDs ({@code @rid}, {@code @in}, {@code @out}) and dropped every property. The rows fed here are the
 * JSON the server writes, so no server is needed: the schema is the one a stub answers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12184RemoteRecordColumnTest {
  private static final String ALICE = "{\"@rid\":\"#1:0\",\"@type\":\"Person\",\"name\":\"alice\",\"age\":30,\"@cat\":\"v\",\"@in\":0,\"@out\":1}";
  private static final String BOB   = "{\"@rid\":\"#1:1\",\"@type\":\"Person\",\"name\":\"bob\",\"age\":40,\"@cat\":\"v\",\"@in\":1,\"@out\":0}";
  private static final String KNOWS = "{\"@rid\":\"#4:0\",\"@type\":\"Knows\",\"since\":2020,\"@cat\":\"e\",\"@in\":\"#1:1\",\"@out\":\"#1:0\"}";

  private static final int LINK = Type.LINK.getId();
  private static final int LIST = Type.LIST.getId();
  private static final int MAP  = Type.MAP.getId();

  /** A database with no server behind it: its schema is a vertex type, an edge type and an embedded document type. */
  private static final class StubbedRemoteDatabase extends RemoteDatabase {
    int schemaReads;

    StubbedRemoteDatabase() {
      super("localhost", 2480, "testdb", "root", "test", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // NO HTTP CALLS IN A UNIT TEST
    }

    @Override
    protected ResultSet commandUncounted(final String language, final String command, final Map<String, Object> params) {
      final InternalResultSet resultSet = new InternalResultSet();
      if (command.contains("schema:types")) {
        ++schemaReads;
        resultSet.add(type("Person", "vertex"));
        resultSet.add(type("Knows", "edge"));
        resultSet.add(type("Address", "document"));
      } else if (command.contains("schema:buckets"))
        resultSet.add(new ResultInternal(Map.of("name", "Person_0")));
      else
        throw new AssertionError("Unexpected command: " + command);
      return resultSet;
    }

    private static ResultInternal type(final String name, final String kind) {
      final Map<String, Object> row = new HashMap<>();
      row.put("name", name);
      row.put("type", kind);
      row.put("records", 0);
      row.put("buckets", List.of(name + "_0"));
      row.put("parentTypes", List.of());
      row.put("properties", List.of());
      return new ResultInternal(row);
    }
  }

  private StubbedRemoteDatabase database;

  @BeforeEach
  void setUp() {
    database = new StubbedRemoteDatabase();
  }

  @AfterEach
  void tearDown() {
    if (database.isOpen())
      database.close();
  }

  @Test
  void cypherVertexAndEdgeColumnsAreRecords() {
    // MATCH (a)-[r:Knows]->(b) RETURN a, r, b
    final Result row = database.json2Result(new JSONObject(
        "{\"a\":" + ALICE + ",\"r\":" + KNOWS + ",\"b\":" + BOB + ",\"@props\":\"a:" + LINK + ",r:" + LINK + ",b:" + LINK + "\"}"));

    assertThat(row.isElement()).isFalse();
    assertThat(row.getPropertyNames()).containsExactly("a", "r", "b");

    final Vertex a = row.getProperty("a");
    assertThat(a.getString("name")).isEqualTo("alice");
    assertThat(a.getInteger("age")).isEqualTo(30);
    assertThat(a.getIdentity()).hasToString("#1:0");

    final Edge r = row.getProperty("r");
    assertThat(r.getInteger("since")).isEqualTo(2020);
    assertThat(r.getOut()).hasToString("#1:0");
    assertThat(r.getIn()).hasToString("#1:1");

    final Vertex b = row.getProperty("b");
    assertThat(b.getString("name")).isEqualTo("bob");
  }

  @Test
  void aRecordColumnNextToAScalarColumn() {
    final Result row = database.json2Result(new JSONObject(
        "{\"a\":" + ALICE + ",\"n\":\"alice\",\"c\":7,\"@props\":\"a:" + LINK + ",c:" + Type.LONG.getId() + "\"}"));

    assertThat(row.<Object>getProperty("a")).isInstanceOf(Vertex.class);
    assertThat(row.<Vertex>getProperty("a").getInteger("age")).isEqualTo(30);
    assertThat(row.<String>getProperty("n")).isEqualTo("alice");
    assertThat(row.<Object>getProperty("c")).as("typed by its own hint").isEqualTo(7L);
  }

  @Test
  void aRecordColumnWithNoHintAtAllIsStillARecord() {
    // includeTypeHints off, or a hint that names no other column: the conversion does not depend on @props
    final Result row = database.json2Result(new JSONObject("{\"me\":" + ALICE + ",\"name\":\"alice\"}"));

    assertThat(row.<Object>getProperty("me")).isInstanceOf(Vertex.class);
    assertThat(row.<String>getProperty("name")).isEqualTo("alice");
  }

  @Test
  void aListOfRecordsColumn() {
    // MATCH (a) RETURN collect(a) AS people
    final Result row = database.json2Result(new JSONObject(
        "{\"people\":[" + ALICE + "," + BOB + "],\"@props\":\"people:" + LIST + "\"}"));

    final List<?> people = row.getProperty("people");
    assertThat(people).hasSize(2).allSatisfy(p -> assertThat(p).isInstanceOf(Vertex.class));
    assertThat(((Vertex) people.get(1)).getString("name")).isEqualTo("bob");
  }

  @Test
  void aListWhoseFirstItemIsNotARecordKeepsEveryRecordItHolds() {
    // a literal [null, a], [1, a], ["#1:0", a]: the first item says nothing about the others
    for (final String first : new String[] { "null", "1", "\"#1:0\"", "\"text\"" }) {
      final Result row = database.json2Result(new JSONObject(
          "{\"mixed\":[" + first + "," + ALICE + "," + BOB + "],\"@props\":\"mixed:" + LIST + "\"}"));

      final List<?> mixed = row.getProperty("mixed");
      assertThat(mixed).as(first).hasSize(3);
      assertThat(mixed.get(1)).as(first).isInstanceOf(Vertex.class);
      assertThat(mixed.get(2)).as(first).isInstanceOf(Vertex.class);
      assertThat(((Vertex) mixed.get(2)).getString("name")).as(first).isEqualTo("bob");
    }
  }

  @Test
  void aListOfListsOfRecords() {
    final Result row = database.json2Result(new JSONObject(
        "{\"nested\":[[" + ALICE + "],[" + BOB + "," + KNOWS + "]],\"@props\":\"nested:" + LIST + "\"}"));

    final List<?> nested = row.getProperty("nested");
    assertThat(nested).hasSize(2);
    assertThat(((List<?>) nested.get(0)).get(0)).isInstanceOf(Vertex.class);
    assertThat(((List<?>) nested.get(1)).get(0)).isInstanceOf(Vertex.class);
    assertThat(((List<?>) nested.get(1)).get(1)).isInstanceOf(Edge.class);
  }

  @Test
  void aMapLiteralHoldingRecords() {
    // RETURN {p: a, n: a.name} AS m
    final Result row = database.json2Result(new JSONObject(
        "{\"m\":{\"p\":" + ALICE + ",\"n\":\"alice\",\"l\":[" + BOB + "]},\"@props\":\"m:" + MAP + "\"}"));

    final Map<?, ?> m = row.getProperty("m");
    assertThat(m.get("p")).isInstanceOf(Vertex.class);
    assertThat(m.get("n")).isEqualTo("alice");
    assertThat(((List<?>) m.get("l")).get(0)).isInstanceOf(Vertex.class);
  }

  @Test
  void aPathColumnHoldsItsNodesAndRelationshipsAsRecords() {
    // MATCH path = (a)-[:Knows]->(b) RETURN path, as serializePath writes it
    final Result row = database.json2Result(new JSONObject("{\"path\":{\"_type\":\"path\",\"nodes\":[" + ALICE + "," + BOB
        + "],\"relationships\":[" + KNOWS + "],\"length\":1}}"));

    final Map<?, ?> path = row.getProperty("path");
    assertThat(path.get("_type")).isEqualTo("path");
    assertThat(path.get("length")).isEqualTo(1);
    final List<?> nodes = (List<?>) path.get("nodes");
    assertThat(nodes).hasSize(2).allSatisfy(n -> assertThat(n).isInstanceOf(Vertex.class));
    assertThat(((List<?>) path.get("relationships")).get(0)).isInstanceOf(Edge.class);
  }

  @Test
  void anEmbeddedDocumentColumnIsADocument() {
    // SELECT address FROM Person: an embedded document is written with its @type and @cat "d", and the hint of its type
    final Result row = database.json2Result(new JSONObject(
        "{\"address\":{\"@type\":\"Address\",\"@cat\":\"d\",\"city\":\"Rome\"},\"@props\":\"address:" + Type.EMBEDDED.getId() + "\"}"));

    assertThat(row.<Object>getProperty("address")).isInstanceOf(Document.class);
    assertThat(row.<Document>getProperty("address").getString("city")).isEqualTo("Rome");
  }

  @Test
  void aListOfRidsKeepsBeingAListOfRids() {
    // a LINK column the server wrote as RIDs, and a list of them: nothing here is a record, and the hint still applies
    final Result row = database.json2Result(new JSONObject(
        "{\"@rid\":\"#1:0\",\"friends\":[\"#1:1\",\"#1:2\"],\"@props\":\"@rid:" + LINK + ",friends:" + LIST + "\"}"));

    assertThat(row.<Object>getProperty("@rid")).hasToString("#1:0");
    assertThat(row.<List<Object>>getProperty("friends")).containsExactly("#1:1", "#1:2");
    assertThat(database.schemaReads).as("no record was built, so the schema was not asked").isZero();
  }

  @Test
  void aMapThatIsNotARecordIsLeftAlone() {
    // @cat is a record's marker only with a category a record has and the @type that names its type: a map literal that
    // happens to carry the key is a map, and building it as a record would ask the server's schema for a type it has not
    final Result row = database.json2Result(new JSONObject(
        "{\"unknownCat\":{\"@cat\":\"x\",\"k\":1},\"noType\":{\"@cat\":\"d\",\"k\":2},\"numericCat\":{\"@cat\":7,\"k\":3},"
            + "\"@props\":\"unknownCat:" + MAP + ",noType:" + MAP + "\"}"));

    assertThat(row.<Map<String, Object>>getProperty("unknownCat")).containsEntry("k", 1);
    assertThat(row.<Map<String, Object>>getProperty("noType")).containsEntry("k", 2);
    assertThat(row.<Map<String, Object>>getProperty("numericCat")).containsEntry("k", 3);
    assertThat(database.schemaReads).as("no record was built, so the schema was not asked").isZero();
  }

  @Test
  void wholeRowRecordsAreUnchanged() {
    final Result row = database.json2Result(new JSONObject(ALICE));

    assertThat(row.isElement()).isTrue();
    assertThat(row.isVertex()).isTrue();
    assertThat(row.getVertex().get().getString("name")).isEqualTo("alice");
  }
}
