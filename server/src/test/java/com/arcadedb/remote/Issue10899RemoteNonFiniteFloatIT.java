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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10899: a FLOAT or DOUBLE holding {@code NaN}, {@code +Infinity} or {@code -Infinity} must read back through
 * the HTTP {@link RemoteDatabase} (type hints on, the default) as the same number. The server writes each as a
 * {@code NonFiniteNumbers} marker, and the client used to hand {@code "PosInfinity"} to {@code Float.parseFloat}, which
 * threw {@code NumberFormatException} - or, for a DOUBLE projection the server sent no hint for, to return the marker
 * itself as a {@code String}.
 */
class Issue10899RemoteNonFiniteFloatIT extends BaseGraphServerTest {

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
