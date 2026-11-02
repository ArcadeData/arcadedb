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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11016: follow-up of #10905. A Cypher row that returns a vertex ({@code RETURN n}) goes through the projection
 * branch of {@code JsonSerializer.serializeResult}, which emitted no type hint, so a non-finite property the schema does
 * not declare reached the Java HTTP client as the marker String. Scalars, lists and maps are plain rows and are pinned too.
 */
class Issue11016RemoteCypherNonFiniteIT extends BaseGraphServerTest {

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
