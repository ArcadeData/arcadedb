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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12046: the Neo4j importer keyed its node-id map by {@code Long.parseLong(id)} while every id seen was numeric, so
 * {@code "1"} and {@code "01"} shared one entry: the relationships of {@code "1"} were attached to the vertex of {@code "01"}.
 * Once a non-numeric id showed up the map was re-keyed with {@code Long.toString}, and every relationship naming {@code "01"}
 * was skipped as "vertex not found". The import reported OK either way.
 */
class Issue12046Neo4jIdSpellingTest {
  private static final String DB_PATH = "target/databases/issue12046";

  private static final String NODES = """
      {"type":"node","id":"1","labels":["P"],"properties":{"name":"one"}}
      {"type":"node","id":"01","labels":["P"],"properties":{"name":"zero-one"}}
      {"type":"node","id":"+1","labels":["P"],"properties":{"name":"plus-one"}}
      {"type":"node","id":"2","labels":["P"],"properties":{"name":"two"}}
      """;

  private static final String RELATIONSHIPS = """
      {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"1","labels":["P"]},"end":{"id":"2","labels":["P"]}}
      {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"01","labels":["P"]},"end":{"id":"2","labels":["P"]}}
      {"type":"relationship","id":"2","label":"R","properties":{"w":3},"start":{"id":"+1","labels":["P"]},"end":{"id":"2","labels":["P"]}}
      """;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  /** Every id parses as a long: "1", "01" and "+1" shared one entry, the last one stored. */
  @Test
  void idsSpellingTheSameNumberStayDistinct() throws Exception {
    assertEveryRelationshipKeepsItsStart(NODES + RELATIONSHIPS);
  }

  /** A non-numeric id after them moved the map to string mode, re-keyed with Long.toString: "01" and "+1" were lost. */
  @Test
  void idsSpellingTheSameNumberStayDistinctAcrossTheMigration() throws Exception {
    assertEveryRelationshipKeepsItsStart(NODES + """
        {"type":"node","id":"x","labels":["P"],"properties":{"name":"ex"}}
        """ + RELATIONSHIPS);
  }

  private static void assertEveryRelationshipKeepsItsStart(final String content) throws Exception {
    final File source = new File("target/issue12046.json");
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      try (final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
        final Result result = rs.next();
        assertThat(result.<Long>getProperty("createdEdges")).isEqualTo(3L);
      }

      final Map<Integer, String> startByWeight = new HashMap<>();
      try (final ResultSet rs = db.query("sql", "SELECT w, @out.name AS start FROM R")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          startByWeight.put(row.<Number>getProperty("w").intValue(), row.getProperty("start"));
        }
      }
      assertThat(startByWeight).containsOnly(Map.entry(1, "one"), Map.entry(2, "zero-one"), Map.entry(3, "plus-one"));
    } finally {
      source.delete();
    }
  }
}
