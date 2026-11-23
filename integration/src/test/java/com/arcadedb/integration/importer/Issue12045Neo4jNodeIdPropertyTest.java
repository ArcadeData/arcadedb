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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12045: the Neo4j importer stores every node's Neo4j id in the {@code id} property, and wrote it over a property of
 * that name the node had of its own (a business key, a UUID), so the node's value was lost and the import reported OK. The
 * node's own value is now kept under {@code _id}, and the report says so.
 */
class Issue12045Neo4jNodeIdPropertyTest {
  private static final String DB_PATH = "target/databases/issue12045";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @Test
  void nodeOwnIdPropertyIsKeptNextToTheNeo4jId() throws Exception {
    final Result result = importNeo4j("issue12045-own-id.json", """
        {"type":"node","id":"0","labels":["User"],"properties":{"id":"user-abc","name":"Adam"}}
        {"type":"node","id":"1","labels":["User"],"properties":{"id":"user-def","name":"Jim"}}
        {"type":"node","id":"2","labels":["User"],"properties":{"name":"Eve"}}
        {"type":"relationship","id":"0","label":"KNOWS","properties":{"id":"rel-x"},"start":{"id":"0","labels":["User"]},"end":{"id":"1","labels":["User"]}}
        """);

    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<Long>getProperty("createdVertices")).isEqualTo(3L);
    assertThat(result.<Long>getProperty("warnings")).isEqualTo(1L);
    assertThat(result.<List<String>>getProperty("warningMessages")).singleElement().asString().contains("'_id'");

    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      final Vertex adam = db.lookupByKey("User", "id", "0").next().asVertex();
      assertThat(adam.getString("name")).isEqualTo("Adam");
      assertThat(adam.getString("_id")).as("the node's own id must survive the import").isEqualTo("user-abc");

      final Vertex jim = db.lookupByKey("User", "id", "1").next().asVertex();
      assertThat(jim.getString("_id")).isEqualTo("user-def");

      final Vertex eve = db.lookupByKey("User", "id", "2").next().asVertex();
      assertThat(eve.has("_id")).as("a node without an id of its own gets no _id").isFalse();

      assertThat(db.query("sql", "SELECT id FROM KNOWS").next().<String>getProperty("id")).isEqualTo("rel-x");
    }
  }

  /** A node with both an id and an _id has nowhere to keep both: refused and counted, never stored without one of them. */
  @Test
  void nodeWithBothIdAndUnderscoreIdIsRefused() throws Exception {
    final Result result = importNeo4j("issue12045-both.json", """
        {"type":"node","id":"0","labels":["User"],"properties":{"id":"user-abc","_id":"other","name":"Adam"}}
        {"type":"node","id":"1","labels":["User"],"properties":{"name":"Jim"}}
        """);

    assertThat(result.<Long>getProperty("createdVertices")).isEqualTo(1L);
    assertThat(result.<Long>getProperty("errors")).isEqualTo(1L);

    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      assertThat(db.countType("User", true)).isEqualTo(1);
      assertThat(db.lookupByKey("User", "id", "1").hasNext()).isTrue();
    }
  }

  private static Result importNeo4j(final String fileName, final String content) throws Exception {
    final File source = new File("target/" + fileName);
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    try (final Database db = new DatabaseFactory(DB_PATH).create();
        final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
      return rs.next();
    } finally {
      source.delete();
    }
  }
}
