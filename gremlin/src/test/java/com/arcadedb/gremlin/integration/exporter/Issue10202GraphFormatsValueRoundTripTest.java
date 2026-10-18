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
import com.arcadedb.gremlin.io.graphml.ArcadeGraphMLReader;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10202: the GraphSON export failed for the whole database on a Byte, Short, BigDecimal, date or byte[] property, and
 * GraphML wrote byte[] and float[] as their Java identity string and read Byte, Short, BigDecimal, dates, lists and maps back as
 * strings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10202GraphFormatsValueRoundTripTest {
  private static final String SOURCE_PATH = "target/databases/issue10202-source";
  private static final String TARGET_PATH = "target/databases/issue10202-target";
  private static final String COPY_PATH   = "target/databases/issue10202-tinkerpop";

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    FileUtils.deleteRecursively(new File(COPY_PATH));
    new File("exports/issue10202.graphson.tgz").delete();
    new File("exports/issue10202.graphml.tgz").delete();
  }

  private static Map<String, Object> values() {
    final Map<String, Object> values = new LinkedHashMap<>();
    values.put("bool", true);
    values.put("byte", (byte) 5);
    values.put("short", (short) 7);
    values.put("int", 42);
    values.put("long", 3L);
    values.put("float", 1.5f);
    values.put("double", 2.5);
    values.put("nan", Double.NaN);
    values.put("inf", Double.POSITIVE_INFINITY);
    values.put("bigdec", new BigDecimal("1.50"));
    values.put("string", "héllo 世界");
    values.put("empty", "");
    values.put("date", new Date(1_700_000_000_000L));
    values.put("ldate", LocalDate.of(2026, 1, 2));
    values.put("ldt", LocalDateTime.of(2026, 1, 2, 3, 4, 5));
    values.put("instant", Instant.ofEpochSecond(1_700_000_000L, 123_456_789L));
    values.put("bin", new byte[] { 1, 2, 3 });
    values.put("floats", new float[] { 1f, 2f });
    values.put("list", new ArrayList<>(List.of(1, "a", 9L, new BigDecimal("0.10"))));
    values.put("map", new LinkedHashMap<>(Map.of("k", "v", "n", 4L)));
    return values;
  }

  @ParameterizedTest
  @ValueSource(strings = { "graphson", "graphml" })
  void everyValueComesBackWithItsType(final String format) throws Exception {
    final Map<String, Object> values = values();
    final String file = "issue10202." + format + ".tgz";

    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE EDGE TYPE E");
      source.transaction(() -> {
        final MutableVertex v = source.newVertex("V").set("id", 1);
        values.forEach(v::set);
        v.save();
        // THE SAME PROPERTY WITH A DIFFERENT TYPE ON ANOTHER VERTEX: ONE GRAPHML KEY, TWO VALUE TYPES
        final MutableVertex w = source.newVertex("V").set("id", 2, "long", "not a number", "int", 7L).save();
        // AND A NULL IN THAT KEY: NOTHING IS WRITTEN FOR IT, SO THE VERTEX COMES BACK WITHOUT A VALUE
        source.newVertex("V").set("id", 3, "long", null).save();
        v.newEdge("E", w, "weight", new BigDecimal("0.75"), "since", LocalDate.of(2020, 5, 6), "b", (byte) 1).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + file + " WITH overwrite = true, format = '" + format + "'");

      try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
        target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + file).getAbsolutePath());

        final Document x = vertex(source, 1);
        final Document y = vertex(target, 1);
        for (final String key : values.keySet())
          assertSame(format + " " + key, x.get(key), y.get(key));

        assertThat(vertex(target, 3).get("long")).as(format + " null").isNull();
        final Document w = vertex(target, 2);
        assertSame(format + " w.long", "not a number", w.get("long"));
        assertSame(format + " w.int", 7L, w.get("int"));

        try (final ResultSet rs = target.query("sql", "SELECT FROM E")) {
          final Document e = rs.next().getElement().get();
          assertSame(format + " e.weight", new BigDecimal("0.75"), e.get("weight"));
          assertSame(format + " e.since", LocalDate.of(2020, 5, 6), e.get("since"));
          assertSame(format + " e.b", (byte) 1, e.get("b"));
        }
      }
    }

    // STILL A FILE TINKERPOP'S OWN GRAPHML READER TAKES: THE TYPES ARE AN ATTRIBUTE OF ANOTHER NAMESPACE, WHICH IT IGNORES. A
    // TYPED GRAPHSON FILE NEEDS ITS TYPES' MODULES IN ANY READER, gx ONES INCLUDED, SO IT HAS NO SUCH CHECK
    if (format.equals("graphml"))
      try (final ArcadeGraph graph = ArcadeGraph.open(new File(COPY_PATH).getAbsolutePath())) {
        try (final GZIPInputStream is = new GZIPInputStream(new FileInputStream("exports/" + file))) {
          graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
        }
        assertThat(graph.getDatabase().countType("V", true)).isEqualTo(3);
        assertThat(graph.getDatabase().countType("E", true)).isEqualTo(1);
        try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT FROM V WHERE id = 1")) {
          // GRAPHML'S OWN TYPES COME BACK AS THEY WERE, THE OTHERS AS THEIR TEXT
          final Document v = rs.next().getElement().get();
          assertThat(v.get("double")).isEqualTo(2.5);
          assertThat(v.get("bigdec")).isEqualTo("1.50");
          assertThat(v.get("bin")).isEqualTo("AQID");
        }
      }
  }

  /**
   * A GraphSON file with ids that are not RIDs is read value by value: a typed value is kept when ArcadeDB can store what it
   * decodes to, converted when it has a stored equivalent, and kept as its text otherwise, never dropped.
   */
  @Test
  void typedValuesOfAForeignGraphSONFileAreKeptOrConverted() throws Exception {
    final File file = new File("target/issue10202-foreign.graphson");
    Files.writeString(file.toPath(), "{\"id\":\"a\",\"label\":\"P\",\"properties\":{\"s\":[{\"id\":\"1\",\"value\":{\"@type\":\"gx:Int16\",\"@value\":7}}],\"buf\":[{\"id\":\"2\",\"value\":{\"@type\":\"gx:ByteBuffer\",\"@value\":\"AQID\"}}],\"c\":[{\"id\":\"3\",\"value\":{\"@type\":\"gx:Char\",\"@value\":\"z\"}}],\"md\":[{\"id\":\"4\",\"value\":{\"@type\":\"gx:MonthDay\",\"@value\":\"--01-02\"}}],\"lst\":[{\"id\":\"5\",\"value\":{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"gx:MonthDay\",\"@value\":\"--03-04\"},{\"@type\":\"gx:Int16\",\"@value\":3}]}}],\"bad\":[{\"id\":\"6\",\"value\":{\"@type\":\"gx:ByteBuffer\",\"@value\":\"%%%\"}}]}}\n");
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      try (final ResultSet rs = target.query("sql", "SELECT FROM P")) {
        final Document p = rs.next().getElement().get();
        assertSame("short", (short) 7, p.get("s"));
        assertSame("byte buffer", new byte[] { 1, 2, 3 }, p.get("buf"));
        assertSame("char", "z", p.get("c"));
        assertSame("month day", "--01-02", p.get("md"));
        // AT ANY DEPTH, AND A VALUE THAT DOES NOT DECODE STAYS THE TEXT IT IS
        assertSame("list", new ArrayList<>(List.of("--03-04", (short) 3)), p.get("lst"));
        assertSame("undecodable", "%%%", p.get("bad"));
      }
    } finally {
      file.delete();
    }
  }

  /** A LocalDate is a known value type, of the type it always resolved to. */
  @Test
  void aLocalDateIsAKnownDateValue() {
    assertThat(Type.getTypeByValue(LocalDate.of(2026, 1, 2))).isEqualTo(Type.DATE);
    assertThat(Type.getTypeByClass(LocalDate.class)).isEqualTo(Type.DATE);
    Type.validateValue(LocalDate.of(2026, 1, 2));
  }

  /** The GraphML reader reads untrusted files: no DTD, so no external entity can pull a local file into a property. */
  @Test
  void anExternalEntityInAGraphMLFileIsNotResolved() throws Exception {
    final File secret = new File("target/issue10202-secret.txt");
    Files.writeString(secret.toPath(), "TOP-SECRET");
    final File graphml = new File("target/issue10202-xxe.graphml");
    Files.writeString(graphml.toPath(), "<?xml version=\"1.0\"?>\n"
        + "<!DOCTYPE graphml [<!ENTITY xxe SYSTEM \"" + secret.toURI() + "\">]>\n"
        + "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">"
        + "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>"
        + "<graph id=\"G\" edgedefault=\"directed\"><node id=\"1\"><data key=\"name\">&xxe;</data></node></graph></graphml>");
    try (final ArcadeGraph graph = ArcadeGraph.open(new File(COPY_PATH).getAbsolutePath())) {
      try (final FileInputStream is = new FileInputStream(graphml)) {
        new ArcadeGraphMLReader().readGraph(is, graph);
      } catch (final Exception e) {
        // REFUSING THE FILE IS FINE TOO: WHAT MUST NOT HAPPEN IS THE FILE'S CONTENT REACHING THE GRAPH
      }
      try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT FROM vertex")) {
        while (rs.hasNext())
          assertThat(String.valueOf(rs.next().<Object>getProperty("name"))).doesNotContain("TOP-SECRET");
      } catch (final Exception e) {
        // NO vertex TYPE: NOTHING WAS IMPORTED
      }
    } finally {
      secret.delete();
      graphml.delete();
    }
  }

  private static Document vertex(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM V WHERE id = ?", id)) {
      return rs.next().getElement().get();
    }
  }

  private static void assertSame(final String what, final Object expected, final Object actual) {
    assertThat(actual).as(what).isNotNull();
    assertThat(actual.getClass()).as(what + " class (" + actual + ")").isEqualTo(expected.getClass());
    if (expected instanceof byte[] b)
      assertThat((byte[]) actual).as(what).isEqualTo(b);
    else if (expected instanceof float[] f)
      assertThat((float[]) actual).as(what).isEqualTo(f);
    else if (expected instanceof Double d && d.isNaN())
      assertThat((Double) actual).as(what).isNaN();
    else if (expected instanceof List<?> list) {
      final List<?> other = (List<?>) actual;
      assertThat(other).as(what).hasSameSizeAs(list);
      for (int i = 0; i < list.size(); i++)
        assertSame(what + "[" + i + "]", list.get(i), other.get(i));
    } else if (expected instanceof Map<?, ?> map) {
      final Map<?, ?> other = (Map<?, ?>) actual;
      assertThat(other.keySet()).as(what).isEqualTo(map.keySet());
      for (final Object key : map.keySet())
        assertSame(what + "[" + key + "]", map.get(key), other.get(key));
    } else
      assertThat(actual).as(what + " " + Arrays.asList(expected, actual)).isEqualTo(expected);
  }
}
