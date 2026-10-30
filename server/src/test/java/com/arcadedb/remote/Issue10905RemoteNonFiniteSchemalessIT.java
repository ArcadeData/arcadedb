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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10905: follow-up of #10899. A non-finite FLOAT/DOUBLE with no schema type behind it - a property of a record
 * that declares none, or an element of a projected LIST/MAP that mixes {@code Float} and {@code Double} - used to
 * reach the Java HTTP client as the {@code NonFiniteNumbers} marker String. A STRING property holding the text of a
 * marker must stay a String.
 */
class Issue10905RemoteNonFiniteSchemalessIT extends BaseGraphServerTest {

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
