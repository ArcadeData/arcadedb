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
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12979: the one-pass read of a row backed by a stored record reads the record's binary content, which a remote record
 * does not have: it holds the properties the server sent in a map, and answers them through its own {@code toMap()} and
 * {@code getPropertyNames()}. A row of a {@link RemoteDatabase} result wraps one of them, and the row serializers must keep
 * answering its properties instead of failing on the missing buffer. The rows fed here are the JSON the server writes, so no
 * server is needed: the schema is the one a stub answers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12979RemoteRecordRowPropertiesTest {
  private static final String ALICE   = "{\"@rid\":\"#1:0\",\"@type\":\"Person\",\"name\":\"alice\",\"age\":30,\"@cat\":\"v\",\"@in\":0,\"@out\":1}";
  private static final String KNOWS   = "{\"@rid\":\"#4:0\",\"@type\":\"Knows\",\"since\":2020,\"@cat\":\"e\",\"@in\":\"#1:1\",\"@out\":\"#1:0\"}";
  private static final String ADDRESS = "{\"@rid\":\"#7:0\",\"@type\":\"Address\",\"city\":\"Rome\",\"zip\":\"00100\",\"@cat\":\"d\"}";

  /** A database with no server behind it: its schema is a vertex type, an edge type and a document type. */
  private static final class StubbedRemoteDatabase extends RemoteDatabase {
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

  // Regression: #12979
  @Test
  void aRowOfARemoteVertexListsItsProperties() {
    final Result row = database.json2Result(new JSONObject(ALICE));
    assertThat(row.getElement().get()).isInstanceOf(RemoteImmutableVertex.class);

    // propertiesOf alone: the JSON of a vertex row also counts its edges, which a remote vertex asks the server for
    assertThat(ResultInternal.propertiesOf(row)).containsExactly(Map.entry("name", "alice"), Map.entry("age", 30));
  }

  // Regression: #12979
  @Test
  void aRowOfARemoteEdgeListsItsProperties() {
    final Result row = database.json2Result(new JSONObject(KNOWS));
    assertThat(row.getElement().get()).isInstanceOf(RemoteImmutableEdge.class);

    assertThat(ResultInternal.propertiesOf(row)).containsExactly(Map.entry("since", 2020));

    final JSONObject json = new JsonSerializer(null).serializeResult(null, row);
    assertThat(json.getInt("since")).isEqualTo(2020);
  }

  // Regression: #12979
  @Test
  void aRowOfARemoteDocumentListsItsProperties() {
    final Result row = database.json2Result(new JSONObject(ADDRESS));
    assertThat(row.getElement().get()).isInstanceOf(RemoteImmutableDocument.class);

    assertThat(ResultInternal.propertiesOf(row)).containsExactly(Map.entry("city", "Rome"), Map.entry("zip", "00100"));

    final JSONObject json = new JsonSerializer(null).serializeResult(null, row);
    assertThat(json.getString("city")).isEqualTo("Rome");
    assertThat(json.getString("zip")).isEqualTo("00100");
  }
}
