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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11131: {@code SELECT @type AS t FROM (SELECT expand(vectorNeighbors(...)))} answered null on every row, while {@code @rid}
 * and the stored properties resolved. The expanded rows wrap no record and carry {@code @type} as a property: {@code @rid} fell
 * back to that property ({@code Result.getIdentity()}), {@code @type}, {@code @in} and {@code @out} asked the record only. The same held for
 * any row that wraps no record, such as the rows of a projected sub-query. Found along the way: {@code ORDER BY @type} read
 * the attribute as a stored property and compared nulls, so it never ordered record rows, and {@code $var.@type} /
 * {@code $var.@rid} read off the context answered null for a record-backed row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11131TypeAttributeOverRowsTest extends TestHelper {
  private static final float[] QUERY = { 1.0f, 0.0f, 0.0f };
  private static final String  VN    = "vectorNeighbors('T[vec]', ?, 3)";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 1");
    database.command("sql", "CREATE PROPERTY T.name STRING");
    database.command("sql", "CREATE PROPERTY T.vec ARRAY_OF_FLOATS");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      final float[][] vectors = { { 1.0f, 0.0f, 0.0f }, { 0.9f, 0.1f, 0.0f }, { 0.0f, 1.0f, 0.0f }, { 0.0f, 0.0f, 1.0f } };
      for (int i = 0; i < vectors.length; i++)
        database.newDocument("T").set("name", "n" + i, "vec", vectors[i]).save();
      database.newVertex("V").set("name", "a").save().newEdge("E", database.newVertex("V").set("name", "b").save()).save();
    });
    database.command("sql", "CREATE INDEX ON T (vec) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }");
  }

  /** The report's case b: the type projected over the expanded neighbours. */
  @Test
  void typeProjectedOverExpandedNeighbours() {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r, @type AS t, name FROM (SELECT expand(" + VN + "))",
        (Object) QUERY)) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r")).isInstanceOf(RID.class);
        assertThat(row.<String>getProperty("t")).isEqualTo("T");
        assertThat(row.<String>getProperty("name")).startsWith("n");
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
  }

  /** The report's case a, the control that already worked: the rows themselves list `@type`. */
  @Test
  void expandedNeighboursCarryTheType() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM (SELECT expand(" + VN + "))", (Object) QUERY)) {
      int rows = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<String>getProperty("@type")).isEqualTo("T");
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
  }

  @Test
  void typeProjectedOverNeighboursWithTheDistance() {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r, @type AS t, distance FROM (SELECT expand(" + VN + "))",
        (Object) QUERY)) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("t")).isEqualTo("T");
        assertThat(row.<Object>getProperty("distance")).isInstanceOf(Number.class);
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
  }

  @Test
  void typeInWhereAndOrderByOverExpandedNeighbours() {
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(" + VN + ")) WHERE @type = 'T' ORDER BY @type, name", (Object) QUERY)) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).hasSize(3);
    }
  }

  /** ORDER BY @type answered null for every row, record or not, so it never ordered anything. */
  @Test
  void orderByTypeOrdersRecordRows() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.command("sql", "CREATE DOCUMENT TYPE C");
    database.transaction(() -> {
      for (final String type : new String[] { "B", "C", "A", "C", "A", "B" })
        database.newDocument(type).save();
    });
    for (final String query : new String[] { "SELECT FROM (SELECT expand(unionall((SELECT FROM B), (SELECT FROM C), (SELECT FROM A)))) ORDER BY @type DESC",
        "SELECT expand(unionall((SELECT FROM B), (SELECT FROM C), (SELECT FROM A))) ORDER BY @type DESC" })
      try (final ResultSet rs = database.query("sql", query)) {
        final List<String> types = new ArrayList<>();
        while (rs.hasNext())
          types.add(rs.next().getElement().orElseThrow().getTypeName());
        assertThat(types).as(query).containsExactly("C", "C", "B", "B", "A", "A");
      }
  }

  @Test
  void orderByTypeOrdersRowsWithoutRecord() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.transaction(() -> {
      for (final String type : new String[] { "A", "B", "A", "B" })
        database.newDocument(type).save();
    });
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(unionall((SELECT @type, @rid FROM A), (SELECT @type, @rid FROM B))) ORDER BY @type DESC")) {
      final List<String> types = new ArrayList<>();
      while (rs.hasNext())
        types.add(rs.next().getProperty("@type"));
      assertThat(types).containsExactly("B", "B", "A", "A");
    }
  }

  /** `$row.@type` over a LET row that wraps no record. */
  @Test
  void typeOfALetRowWithoutRecord() {
    try (final ResultSet rs = database.query("sql",
        "SELECT $row.@type AS t, $row.@rid AS r FROM T LET $row = first((SELECT @type, @rid FROM T LIMIT 1)) LIMIT 1")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("t")).isEqualTo("T");
      assertThat(row.<Object>getProperty("r")).isInstanceOf(RID.class);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A dotted variable path read off the context answers `@type` of a row that wraps no record, as it answers `@rid`. */
  @Test
  void variablePathReadsTheTypeOfARowWithoutRecord() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final ResultInternal row = new ResultInternal(database);
    row.setProperty("@type", "T");
    context.setVariable("row", row);
    assertThat(context.getVariable("$row.@type")).isEqualTo("T");

    final Document record = database.iterateType("T", false).next().asDocument();
    context.setVariable("rec", new ResultInternal(record));
    assertThat(context.getVariable("$rec.@type")).isEqualTo("T");
    assertThat(context.getVariable("$rec.@rid")).isEqualTo(record.getIdentity());

    assertThat(context.getVariablePath("$row.@type")).isEqualTo("T");
    assertThat(context.getVariablePath("$rec.@type")).isEqualTo("T");
    assertThat(context.getVariablePath("$rec.@rid")).isEqualTo(record.getIdentity());
  }

  /** Not vector-specific: any row that wraps no record and carries `@type` answers it. */
  @Test
  void typeProjectedOverAProjectedSubQuery() {
    try (final ResultSet rs = database.query("sql", "SELECT @type AS t, @rid AS r FROM (SELECT @rid, @type, name FROM T)")) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("t")).isEqualTo("T");
        assertThat(row.<Object>getProperty("r")).isInstanceOf(RID.class);
        rows++;
      }
      assertThat(rows).isEqualTo(4);
    }
  }

  @Test
  void typeAsMethodTargetOverAProjectedSubQuery() {
    try (final ResultSet rs = database.query("sql", "SELECT @type.toLowerCase() AS t FROM (SELECT @type, name FROM T) WHERE @type = 'T'")) {
      int rows = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<String>getProperty("t")).isEqualTo("t");
        rows++;
      }
      assertThat(rows).isEqualTo(4);
    }
  }

  @Test
  void edgeEndpointsProjectedOverAProjectedSubQuery() {
    try (final ResultSet rs = database.query("sql", "SELECT @in AS i, @out AS o, @type AS t FROM (SELECT @in, @out, @type FROM E)")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("i")).isInstanceOf(RID.class);
      assertThat(row.<Object>getProperty("o")).isInstanceOf(RID.class);
      assertThat(row.<String>getProperty("t")).isEqualTo("E");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A row that wraps no record and carries no `@type` still answers null, rather than failing. */
  @Test
  void rowWithoutTypeAnswersNull() {
    try (final ResultSet rs = database.query("sql", "SELECT @type AS t, @in AS i FROM (SELECT name FROM T LIMIT 1)")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("t")).isNull();
      assertThat(row.<Object>getProperty("i")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** The controls of the report: a record-backed row was always right. */
  @Test
  void typeProjectedOverRecordsIsUnchanged() {
    try (final ResultSet rs = database.query("sql", "SELECT @type AS t, @rid AS r FROM (SELECT FROM T LIMIT 3)")) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("t")).isEqualTo("T");
        assertThat(row.<Object>getProperty("r")).isInstanceOf(RID.class);
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
  }
}
