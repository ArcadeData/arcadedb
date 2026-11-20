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

  /** "0" and "-0" parse to the same long, and "-0" is not the way Long.toString spells it. */
  @Test
  void negativeZeroIsNotZero() throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts("""
        {"type":"node","id":"0","labels":["P"],"properties":{"name":"zero"}}
        {"type":"node","id":"-0","labels":["P"],"properties":{"name":"minus-zero"}}
        {"type":"node","id":"2","labels":["P"],"properties":{"name":"two"}}
        {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"0","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"-0","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        """, 2L);
    assertThat(startByWeight).containsOnly(Map.entry(1, "zero"), Map.entry(2, "minus-zero"));
  }

  /** While every id is canonical the map stays numeric: zero, a negative id and the largest long keep their own entries. */
  @Test
  void canonicalIdsKeepTheirOwnEntries() throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts("""
        {"type":"node","id":"0","labels":["P"],"properties":{"name":"zero"}}
        {"type":"node","id":"-5","labels":["P"],"properties":{"name":"minus-five"}}
        {"type":"node","id":"9223372036854775807","labels":["P"],"properties":{"name":"max"}}
        {"type":"node","id":"7","labels":["P"],"properties":{"name":"seven"}}
        {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"0","labels":["P"]},"end":{"id":"7","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"-5","labels":["P"]},"end":{"id":"7","labels":["P"]}}
        {"type":"relationship","id":"2","label":"R","properties":{"w":3},"start":{"id":"9223372036854775807","labels":["P"]},"end":{"id":"7","labels":["P"]}}
        """, 3L);
    assertThat(startByWeight).containsOnly(Map.entry(1, "zero"), Map.entry(2, "minus-five"), Map.entry(3, "max"));
  }

  /** A relationship naming a spelling no node has finds nothing: "07" is not the node "7", although both parse to seven. */
  @Test
  void aSpellingNoNodeHasIsNotTheNodeItParsesTo() throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts("""
        {"type":"node","id":"7","labels":["P"],"properties":{"name":"seven"}}
        {"type":"node","id":"8","labels":["P"],"properties":{"name":"eight"}}
        {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"07","labels":["P"]},"end":{"id":"8","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"7","labels":["P"]},"end":{"id":"8","labels":["P"]}}
        """, 1L);
    assertThat(startByWeight).containsOnly(Map.entry(2, "seven"));
  }

  /**
   * Long.MIN_VALUE is spelled canonically, but it is the empty marker of the primitive map, which refuses it as a key: the
   * vertex was created and left out of the map, and every relationship naming it was skipped.
   */
  @Test
  void theSmallestLongIsKept() throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts("""
        {"type":"node","id":"1","labels":["P"],"properties":{"name":"one"}}
        {"type":"node","id":"-9223372036854775808","labels":["P"],"properties":{"name":"min"}}
        {"type":"node","id":"2","labels":["P"],"properties":{"name":"two"}}
        {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"1","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"-9223372036854775808","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        """, 2L);
    assertThat(startByWeight).containsOnly(Map.entry(1, "one"), Map.entry(2, "min"));
  }

  /** An id past the range of a long cannot be a primitive key: it moves the map to string mode, and the earlier ids follow. */
  @Test
  void anIdPastTheRangeOfALongMigratesTheEarlierOnes() throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts("""
        {"type":"node","id":"1","labels":["P"],"properties":{"name":"one"}}
        {"type":"node","id":"9223372036854775808","labels":["P"],"properties":{"name":"past-max"}}
        {"type":"node","id":"2","labels":["P"],"properties":{"name":"two"}}
        {"type":"relationship","id":"0","label":"R","properties":{"w":1},"start":{"id":"1","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{"w":2},"start":{"id":"9223372036854775808","labels":["P"]},"end":{"id":"2","labels":["P"]}}
        """, 2L);
    assertThat(startByWeight).containsOnly(Map.entry(1, "one"), Map.entry(2, "past-max"));
  }

  private static void assertEveryRelationshipKeepsItsStart(final String content) throws Exception {
    final Map<Integer, String> startByWeight = importAndReadStarts(content, 3L);
    assertThat(startByWeight).containsOnly(Map.entry(1, "one"), Map.entry(2, "zero-one"), Map.entry(3, "plus-one"));
  }

  /** Imports {@code content} and answers the name of the vertex each relationship starts at, by the weight {@code w} it carries. */
  private static Map<Integer, String> importAndReadStarts(final String content, final long expectedEdges) throws Exception {
    final File source = new File("target/issue12046.json");
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      try (final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
        final Result result = rs.next();
        assertThat(result.<Long>getProperty("createdEdges")).isEqualTo(expectedEdges);
      }

      final Map<Integer, String> startByWeight = new HashMap<>();
      try (final ResultSet rs = db.query("sql", "SELECT w, @out.name AS start FROM R")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          startByWeight.put(row.<Number>getProperty("w").intValue(), row.getProperty("start"));
        }
      }
      return startByWeight;
    } finally {
      source.delete();
    }
  }
}
