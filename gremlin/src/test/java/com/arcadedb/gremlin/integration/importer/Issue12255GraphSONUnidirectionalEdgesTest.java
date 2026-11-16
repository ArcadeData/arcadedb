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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12255: a GraphSON export writes an edge of a UNIDIRECTIONAL type in the {@code outE} of its source only (its target
 * keeps no IN list), and the import created edges from the {@code inE} entries only, so a GraphSON round trip silently dropped
 * every unidirectional edge, and the import of any GraphSON whose {@code inE} misses an edge lost that edge too.
 */
class Issue12255GraphSONUnidirectionalEdgesTest {
  private static final String SOURCE_PATH = "target/databases/issue12255-source";
  private static final String TARGET_PATH = "target/databases/issue12255-target";
  private static final String FILE        = "issue12255.graphson.tgz";
  private static final File   HAND_WRITTEN = new File("target/issue12255-outE-only.graphson");

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
    HAND_WRITTEN.delete();
  }

  private static long count(final Database database, final String type) {
    if (!database.getSchema().existsType(type))
      return -1;
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:" + type + "]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void roundTripKeepsUnidirectionalEdges() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE EDGE TYPE F");
      source.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
      source.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
      source.transaction(() -> {
        final MutableVertex a = source.newVertex("V").set("name", "a").save();
        final MutableVertex b = source.newVertex("V").set("name", "b").save();
        a.newEdge("F", b, "w", 1);
        a.newEdge("U", b, "w", 2);
        b.newEdge("U", b, "w", 3);
        a.newEdge("L", b);
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true, format = 'graphson'");

      try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
        target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());

        assertThat(count(target, "F")).isEqualTo(1L);
        assertThat(count(target, "U")).isEqualTo(2L);
        assertThat(count(target, "L")).isEqualTo(1L);
        try (final ResultSet rs = target.query("opencypher",
            "MATCH (x)-[r:U]->(y) RETURN x.name AS x, y.name AS y, r.w AS w ORDER BY w")) {
          assertThat(rs.next().<String>getProperty("x")).isEqualTo("a");
          final var selfLoop = rs.next();
          assertThat(selfLoop.<String>getProperty("x")).isEqualTo("b");
          assertThat(selfLoop.<String>getProperty("y")).isEqualTo("b");
          assertThat(selfLoop.<Number>getProperty("w").intValue()).isEqualTo(3);
        }
        try (final ResultSet rs = target.command("sql", "CHECK DATABASE")) {
          assertThat(rs.next().<Long>getProperty("totalErrors")).isZero();
        }
      }
    }
  }

  /** The target vertex has no inE key at all: the only edge into it is listed in the outE of its source. */
  @Test
  void targetWithoutInEIsImported() throws Exception {
    Files.writeString(HAND_WRITTEN.toPath(), """
        {"id":"#1:0","label":"V","outE":{"R":[{"id":"#9:0","inV":"#1:1","properties":{"w":{"@type":"g:Int32","@value":5}}}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"a"}]}}
        {"id":"#1:1","label":"V","properties":{"name":[{"id":{"@type":"g:Int64","@value":1},"value":"b"}]}}
        """, StandardCharsets.UTF_8);
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + HAND_WRITTEN.getAbsolutePath());
      assertThat(count(target, "R")).isEqualTo(1L);
      try (final ResultSet rs = target.query("opencypher", "MATCH (x {name: 'a'})-[r:R]->(y {name: 'b'}) WHERE r.w = 5 RETURN count(r) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
    }
  }

  /** A producer that lists an edge in the outE of its source only, with RID-shaped ids as an ArcadeDB export has them. */
  @Test
  void edgeListedOnlyInOutEIsImported() throws Exception {
    Files.writeString(HAND_WRITTEN.toPath(), """
        {"id":"#1:0","label":"V","outE":{"R":[{"id":"#9:0","inV":"#1:1","properties":{"w":{"@type":"g:Int32","@value":7}}},{"id":"#9:1","inV":"#1:1"}]},"inE":{"R":[{"id":"#9:2","outV":"#1:1"}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"a"}]}}
        {"id":"#1:1","label":"V","outE":{"R":[{"id":"#9:2","inV":"#1:0"}]},"inE":{"R":[{"id":"#9:1","outV":"#1:0"}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":1},"value":"b"}]}}
        """, StandardCharsets.UTF_8);
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + HAND_WRITTEN.getAbsolutePath());
      assertThat(count(target, "R")).isEqualTo(3L);
      try (final ResultSet rs = target.query("opencypher", "MATCH (x {name: 'a'})-[r:R]->(y {name: 'b'}) WHERE r.w = 7 RETURN count(r) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
    }
  }
}
