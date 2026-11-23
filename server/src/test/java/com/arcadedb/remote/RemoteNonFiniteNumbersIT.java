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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A FLOAT or DOUBLE holding {@code NaN}, {@code +Infinity} or {@code -Infinity} reads back through the HTTP
 * {@link RemoteDatabase} as the same number: declared properties, schemaless properties and collections, and Cypher rows.
 * <p>
 * Each nested class keeps its own server fixture (data created per test), so they cannot share one.
 */
class RemoteNonFiniteNumbersIT {

  /**
   * Issue #10899: a FLOAT or DOUBLE holding {@code NaN}, {@code +Infinity} or {@code -Infinity} must read back through
   * the HTTP {@link RemoteDatabase} (type hints on, the default) as the same number. The server writes each as a
   * {@code NonFiniteNumbers} marker, and the client used to hand {@code "PosInfinity"} to {@code Float.parseFloat}, which
   * threw {@code NumberFormatException} - or, for a DOUBLE projection the server sent no hint for, to return the marker
   * itself as a {@code String}.
   */
  @Nested
  class DeclaredFloatAndDouble extends BaseGraphServerTest {

    private static final float[]  FLOATS  = { Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN };
    private static final double[] DOUBLES = { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN };

    private RemoteDatabase remote;

    @Override
    protected String getDatabaseName() {
      return "issue10899";
    }

    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE DOCUMENT TYPE M");
      database.command("sql", "CREATE PROPERTY M.i INTEGER");
      database.command("sql", "CREATE PROPERTY M.f FLOAT");
      database.command("sql", "CREATE PROPERTY M.d DOUBLE");
      database.command("sql", "CREATE PROPERTY M.fl LIST OF FLOAT");
      database.command("sql", "CREATE PROPERTY M.dm MAP OF DOUBLE");
      database.transaction(() -> {
        for (int i = 0; i < FLOATS.length; i++) {
          final MutableDocument doc = database.newDocument("M");
          doc.set("i", i);
          doc.set("f", FLOATS[i]);
          doc.set("d", DOUBLES[i]);
          doc.set("fl", List.of(FLOATS[i], 1.5F));
          final Map<String, Object> map = new HashMap<>();
          map.put("k", DOUBLES[i]);
          doc.set("dm", map);
          doc.save();
        }
      });

      remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    @AfterEach
    void closeRemote() {
      if (remote != null)
        remote.close();
    }

    @Test
    void projectionReadsNonFiniteFloatAndDouble() {
      try (final ResultSet rs = remote.query("sql", "SELECT i, f, d FROM M ORDER BY i")) {
        for (int i = 0; i < FLOATS.length; i++) {
          final Result row = rs.next();
          assertThat(row.<Object>getProperty("f")).isInstanceOf(Float.class).isEqualTo(FLOATS[i]);
          assertThat(row.<Object>getProperty("d")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
        }
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void computedProjectionReadsNonFiniteDouble() {
      // no schema type for the column: the hint comes from the value's class alone
      try (final ResultSet rs = remote.query("sql", "SELECT i, d * 2 AS twice FROM M ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++) {
          final Object twice = rs.next().getProperty("twice");
          assertThat(twice).isInstanceOf(Double.class).isEqualTo(DOUBLES[i] * 2);
        }
      }
    }

    @Test
    void recordReadsNonFiniteFloatAndDouble() {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM M ORDER BY i")) {
        for (int i = 0; i < FLOATS.length; i++) {
          final Document doc = rs.next().getElement().get().asDocument();
          assertThat(doc.get("f")).isInstanceOf(Float.class).isEqualTo(FLOATS[i]);
          assertThat(doc.get("d")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
          assertThat(doc.getList("fl")).containsExactly(FLOATS[i], 1.5F);
          assertThat(doc.getMap("dm")).containsEntry("k", DOUBLES[i]);
        }
      }
    }

    @Test
    void projectedCollectionsReadNonFiniteElements() {
      try (final ResultSet rs = remote.query("sql", "SELECT i, fl, dm FROM M ORDER BY i")) {
        for (int i = 0; i < FLOATS.length; i++) {
          final Result row = rs.next();
          final List<Object> list = row.getProperty("fl");
          assertThat(list).containsExactly(FLOATS[i], 1.5F);
          final Map<?, ?> map = row.getProperty("dm");
          assertThat(map.get("k")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
        }
      }
    }
  }

  /**
   * Issue #10905: follow-up of #10899. A non-finite FLOAT/DOUBLE with no schema type behind it - a property of a record
   * that declares none, or an element of a projected LIST/MAP that mixes {@code Float} and {@code Double} - used to
   * reach the Java HTTP client as the {@code NonFiniteNumbers} marker String. A STRING property holding the text of a
   * marker must stay a String.
   */
  @Nested
  class Schemaless extends BaseGraphServerTest {

    private static final double[] DOUBLES = { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN };
    private static final float[]  FLOATS  = { Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN };

    private RemoteDatabase remote;

    @Override
    protected String getDatabaseName() {
      return "issue10905";
    }

    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE PROPERTY S.i INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE Partial");
      database.command("sql", "CREATE PROPERTY Partial.i INTEGER");
      database.command("sql", "CREATE PROPERTY Partial.declared DOUBLE");
      database.command("sql", "CREATE VERTEX TYPE SV");
      database.transaction(() -> {
        for (int i = 0; i < DOUBLES.length; i++) {
          final MutableDocument doc = database.newDocument("S");
          doc.set("i", i);
          doc.set("d", DOUBLES[i]);
          doc.set("f", FLOATS[i]);
          doc.set("text", "NaN");
          doc.set("fin", 1.5D);
          final List<Object> mixed = new ArrayList<>();
          mixed.add(FLOATS[i]);
          mixed.add(2.5D);
          mixed.add(3);
          doc.set("mixedList", mixed);
          final Map<String, Object> map = new HashMap<>();
          map.put("a", DOUBLES[i]);
          map.put("b", 1.5F);
          doc.set("mixedMap", map);
          final Map<String, Object> embedded = new HashMap<>();
          embedded.put("x", DOUBLES[i]);
          doc.set("embeddedMap", embedded);
          doc.save();

          final MutableDocument partial = database.newDocument("Partial");
          partial.set("i", i);
          partial.set("declared", DOUBLES[i]);
          partial.set("undeclared", DOUBLES[i]);
          partial.save();

          database.newVertex("SV").set("i", i).set("d", DOUBLES[i]).save();
        }
      });

      remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    @AfterEach
    void closeRemote() {
      if (remote != null)
        remote.close();
    }

    @Test
    void schemalessRecordPropertyReadsNonFinite() {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM S ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++) {
          final Document doc = rs.next().getElement().get().asDocument();
          assertThat(doc.get("d")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
          assertThat(doc.get("f")).isInstanceOf(Float.class).isEqualTo(FLOATS[i]);
          // a string that merely looks like a marker is still a string, and a finite double is untouched
          assertThat(doc.get("text")).isEqualTo("NaN");
          assertThat(doc.get("fin")).isEqualTo(1.5D);
        }
      }
    }

    @Test
    void schemalessCollectionsMixingFloatAndDoubleReadNonFinite() {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM S ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++) {
          final Document doc = rs.next().getElement().get().asDocument();
          assertThat(doc.getList("mixedList")).containsExactly((double) FLOATS[i], 2.5D, 3.0D);
          assertThat(doc.getMap("mixedMap").get("a")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
          assertThat(doc.getMap("embeddedMap").get("x")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
        }
      }
    }

    @Test
    void projectedMixedCollectionsReadNonFinite() {
      try (final ResultSet rs = remote.query("sql", "SELECT i, mixedList, mixedMap FROM S ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++) {
          final Result row = rs.next();
          final List<Object> list = row.getProperty("mixedList");
          assertThat(list).containsExactly((double) FLOATS[i], 2.5D, 3.0D);
          final Map<?, ?> map = row.getProperty("mixedMap");
          assertThat(map.get("a")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
        }
      }
    }

    @Test
    void declaredAndUndeclaredPropertiesOfTheSameRecord() {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM Partial ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++) {
          final Document doc = rs.next().getElement().get().asDocument();
          assertThat(doc.get("declared")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
          assertThat(doc.get("undeclared")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
        }
      }
    }

    @Test
    void schemalessVertexPropertyReadsNonFinite() {
      try (final ResultSet rs = remote.query("sql", "SELECT FROM SV ORDER BY i")) {
        for (int i = 0; i < DOUBLES.length; i++)
          assertThat(rs.next().getVertex().get().get("d")).isInstanceOf(Double.class).isEqualTo(DOUBLES[i]);
      }
    }
  }

  /**
   * Issue #11016: follow-up of #10905. A Cypher row that returns a vertex ({@code RETURN n}) goes through the projection
   * branch of {@code JsonSerializer.serializeResult}, which emitted no type hint, so a non-finite property the schema does
   * not declare reached the Java HTTP client as the marker String. Scalars, lists and maps are plain rows and are pinned too.
   */
  @Nested
  class CypherRows extends BaseGraphServerTest {

    private RemoteDatabase remote;

    @Override
    protected String getDatabaseName() {
      return "issue11016";
    }

    @BeforeEach
    void createData() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE VERTEX TYPE NF");
      database.command("sql", "CREATE PROPERTY NF.declared DOUBLE");
      database.transaction(() -> database.newVertex("NF")
          .set("declared", Double.NEGATIVE_INFINITY)
          .set("d", Double.POSITIVE_INFINITY)
          .set("f", Float.NaN)
          .set("list", List.of(Float.NaN, 2.5D, 3))
          .set("map", Map.of("a", Double.NEGATIVE_INFINITY, "b", 1.5F))
          .set("text", "NaN")
          .set("aSet", new LinkedHashSet<>(List.of(Double.NaN, 1.5D)))
          .save());
      remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    @AfterEach
    void closeRemote() {
      if (remote != null)
        remote.close();
    }

    @Test
    void returnedVertexReadsNonFiniteProperties() {
      try (final ResultSet rs = remote.query("opencypher", "MATCH (n:NF) RETURN n")) {
        final Document doc = rs.next().getElement().get().asDocument();
        assertThat(doc.get("declared")).isInstanceOf(Double.class).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(doc.get("d")).isInstanceOf(Double.class).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(doc.get("f")).isInstanceOf(Float.class).isEqualTo(Float.NaN);
        assertThat(doc.getList("list")).containsExactly((double) Float.NaN, 2.5D, 3.0D);
        assertThat(doc.getMap("map").get("a")).isInstanceOf(Double.class).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(doc.get("text")).isEqualTo("NaN");
        assertThat(doc.get("aSet")).isNotNull();
      }
    }

    @Test
    void scalarListAndMapProjectionsReadNonFinite() {
      try (final ResultSet rs = remote.query("opencypher",
          "MATCH (n:NF) RETURN n.d AS d, n.f AS f, n.list AS list, n.map AS map, [n.d, 1.5] AS lit, {k: n.d} AS m")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("d")).isInstanceOf(Double.class).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(row.<Object>getProperty("f")).isInstanceOf(Float.class);
        assertThat(((Float) row.getProperty("f")).isNaN()).isTrue();
        assertThat(row.<List<Object>>getProperty("list")).containsExactly((double) Float.NaN, 2.5D, 3.0D);
        assertThat(((Map<?, ?>) row.getProperty("map")).get("a")).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(row.<List<Object>>getProperty("lit")).containsExactly(Double.POSITIVE_INFINITY, 1.5D);
        assertThat(((Map<?, ?>) row.getProperty("m")).get("k")).isEqualTo(Double.POSITIVE_INFINITY);
      }
    }
  }
}
