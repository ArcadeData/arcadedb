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
package com.arcadedb.gremlin.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12761: the GraphML writer emitted text XML 1.0 cannot carry (a control character, U+FFFE, an unpaired surrogate) as it
 * was, so the export reported success and the file could not be parsed; a carriage return in a value, and a tab, line feed or
 * carriage return in a property name, were read back as something else.
 */
class Issue12761GraphMLXmlCharactersTest {
  private static final String SOURCE_PATH = "target/databases/issue12761-source";
  private static final String TARGET_PATH = "target/databases/issue12761-target";
  private static final String COPY_PATH   = "target/databases/issue12761-tinkerpop";

  /**
   * Values that are exact in GraphSON and JSONL: every character class XML has to treat on its own. No lone surrogate: the database
   * itself refuses to store one.
   */
  private static final String[] STRINGS = { "a\u0001b", "x\u0000y", "vt\u000bf\u000cx", "esc\u001b[0m", "bad\uFFFEend", "bad\uFFFFend",
      "cr\rlf", "cr\r\nlf", "\r", "tab\tnl\nx", "pair \uD83D\uDE00 ok", "quote \" and \\ back \\u0041",
      "<tag attr='1'>&amp;</tag>", "  padded  ", "" };

  /** Property names an attribute value cannot carry as they are. */
  private static final String[] NAMES = { "k\u0001ctl", "k\ttab", "k\nnl", "k\rcr", "k\uFFFEbad", "k\"quote", "k<lt>&amp" };

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    FileUtils.deleteRecursively(new File(COPY_PATH));
    new File("exports/issue12761.graphml.tgz").delete();
    new File("exports/issue12761.graphson.tgz").delete();
  }

  @ParameterizedTest
  @ValueSource(strings = { "graphml", "graphson" })
  void stringsAndPropertyNamesXmlCannotCarryComeBackExact(final String format) throws Exception {
    final String file = "issue12761." + format + ".tgz";

    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE EDGE TYPE E");
      source.transaction(() -> {
        for (int i = 0; i < STRINGS.length; i++)
          source.newVertex("V").set("id", i, "s", STRINGS[i]).save();

        // THE NAMES, ON ONE VERTEX AND ON AN EDGE
        final MutableVertex named = source.newVertex("V").set("id", 100);
        for (int i = 0; i < NAMES.length; i++)
          named.set(NAMES[i], "value" + i);
        named.save();
        final MutableVertex other = source.newVertex("V").set("id", 101).save();
        named.newEdge("E", other, NAMES[0], "edge\u0001value", "cr", "a\rb").save();

        // A LIST AND A MAP ARE JSON TEXT: THEIR STRINGS AND KEYS CARRY THE SAME CHARACTERS
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("k\u0001", "v\uFFFE");
        map.put("plain", "cr\r");
        source.newVertex("V").set("id", 102, "list", new ArrayList<>(List.of("a\u0001", "b\uFFFEc", "cr\r", 5)), "map", map).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + file + " WITH overwrite = true, format = '" + format + "'");

      try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
        target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + file).getAbsolutePath());

        assertThat(target.countType("V", true)).isEqualTo(STRINGS.length + 3);
        for (int i = 0; i < STRINGS.length; i++)
          // WHAT THE SOURCE HOLDS, NOT THE LITERAL: A CHARACTER THE DATABASE DOES NOT STORE AS WRITTEN IS NOT THE EXPORT'S TO RESTORE
          assertThat((Object) vertex(target, i).get("s")).as(format + " value " + i).isEqualTo(vertex(source, i).get("s"));

        final Document named = vertex(target, 100);
        for (int i = 0; i < NAMES.length; i++)
          assertThat((Object) named.get(NAMES[i])).as(format + " property " + i).isEqualTo("value" + i);
        assertThat(named.getPropertyNames()).as(format + " property names").containsExactlyInAnyOrder(
            concat("id", NAMES));

        try (final ResultSet rs = target.query("sql", "SELECT FROM E")) {
          final Document edge = rs.next().getElement().get();
          assertThat((Object) edge.get(NAMES[0])).isEqualTo("edge\u0001value");
          assertThat((Object) edge.get("cr")).isEqualTo("a\rb");
        }

        final Document container = vertex(target, 102);
        assertThat((Object) container.get("list")).as(format + " list").isEqualTo(List.of("a\u0001", "b\uFFFEc", "cr\r", 5));
        final Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("k\u0001", "v\uFFFE");
        expectedMap.put("plain", "cr\r");
        assertThat((Object) container.get("map")).as(format + " map").isEqualTo(expectedMap);
      }
    }
  }

  /** The file stays a file TinkerPop's own GraphML reader takes: the stand-ins and the escapes are plain text to it. */
  @Test
  void theFileIsStillWellFormedGraphMLForAnyReader() throws Exception {
    final String format = "graphml";
    final String file = "issue12761." + format + ".tgz";

    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.transaction(() -> {
        source.newVertex("V").set("id", 1, "s", "a\u0001b", NAMES[0], "x", NAMES[1], "y").save();
        source.newVertex("V").set("id", 2, "s", "cr\rlf").save();
      });
      source.command("sql", "EXPORT DATABASE file://" + file + " WITH overwrite = true, format = '" + format + "'");
    }

    try (final ArcadeGraph graph = ArcadeGraph.open(new File(COPY_PATH).getAbsolutePath())) {
      try (final GZIPInputStream is = new GZIPInputStream(new FileInputStream("exports/" + file))) {
        graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
      }
      assertThat(graph.getDatabase().countType("V", true)).isEqualTo(2);
      try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT FROM V WHERE id = 2")) {
        // A CARRIAGE RETURN IS A CHARACTER REFERENCE, SO ANY READER GETS IT BACK
        assertThat((Object) rs.next().getElement().get().get("s")).isEqualTo("cr\rlf");
      }
    }
  }

  private static String[] concat(final String first, final String[] rest) {
    final String[] all = new String[rest.length + 1];
    all[0] = first;
    System.arraycopy(rest, 0, all, 1, rest.length);
    return all;
  }

  private static Document vertex(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM V WHERE id = ?", id)) {
      return rs.next().getElement().get();
    }
  }
}
