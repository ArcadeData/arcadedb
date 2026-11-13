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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.OrderByItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.MaterializedViewQueryClassifier;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlProjectionTest {
  /**
   * Regression for #11131.
   * <p>
   * Issue #11131: {@code SELECT @type AS t FROM (SELECT expand(vectorNeighbors(...)))} answered null on every row, while {@code @rid}
   * and the stored properties resolved. The expanded rows wrap no record and carry {@code @type} as a property: {@code @rid} fell
   * back to that property ({@code Result.getIdentity()}), {@code @type}, {@code @in} and {@code @out} asked the record only. The same held for
   * any row that wraps no record, such as the rows of a projected sub-query. Found along the way: {@code ORDER BY @type} read
   * the attribute as a stored property and compared nulls, so it never ordered record rows, and {@code $var.@type} /
   * {@code $var.@rid} read off the context answered null for a record-backed row.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeAttributeOverRows extends TestHelper {
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
          "SELECT name FROM (SELECT expand(" + VN + ")) WHERE @type = 'T' ORDER BY @type, name DESC", (Object) QUERY)) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
        // every row is of type T, so the second key alone orders them
        assertThat(names).hasSize(3).contains("n0", "n1").isSortedAccordingTo(Comparator.reverseOrder());
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

    /** ORDER BY @out / @in over edge records orders by the endpoint RIDs, as ORDER BY @rid does. */
    @Test
    void orderByEdgeEndpointsOrdersEdgeRows() {
      database.transaction(() -> {
        final Vertex hub = database.newVertex("V").set("name", "hub").save();
        for (int i = 0; i < 4; i++)
          database.newVertex("V").set("name", "s" + i).save().newEdge("E", hub).save();
      });
      for (final String attribute : new String[] { "@out", "@in" })
        for (final String direction : new String[] { "ASC", "DESC" }) {
          final String query = "SELECT FROM E ORDER BY " + attribute + " " + direction + ", @rid";
          try (final ResultSet rs = database.query("sql", query)) {
            final List<RID> endpoints = new ArrayList<>();
            while (rs.hasNext()) {
              final Edge edge = rs.next().getElement().orElseThrow().asEdge();
              endpoints.add("@out".equals(attribute) ? edge.getOut() : edge.getIn());
            }
            assertThat(endpoints).as(query).hasSize(5)
                .isSortedAccordingTo("ASC".equals(direction) ? Comparator.<RID>naturalOrder() : Comparator.<RID>reverseOrder());
          }
        }
    }

    /** A column of the attribute's name the row carries wins over the record's attribute, a null one included. */
    @Test
    void orderByTypeKeepsAProjectedNull() {
      final Document record = database.iterateType("T", false).next().asDocument();
      // a projection row that keeps its record: content plus element
      final ResultInternal projectedNull = new ResultInternal(database).setElement(record);
      projectedNull.setProperty("@type", null);
      final ResultInternal plain = new ResultInternal(record);

      final OrderByItem item = new OrderByItem();
      item.setRecordAttr("@type");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      // null sorts first; reading through to the record would answer "T" for both and compare them equal
      assertThat(item.compare(projectedNull, plain, context)).isNegative();
      assertThat(item.compare(plain, plain, context)).isZero();

      // the context variable paths read the attribute the same way: the column first, then the record
      context.setVariable("p", projectedNull);
      assertThat(context.getVariable("$p.@type")).isNull();
      assertThat(context.getVariablePath("$p.@type")).isNull();
      assertThat(context.getVariable("$p.@rid")).isEqualTo(record.getIdentity());
      assertThat(context.getVariablePath("$p.@rid")).isEqualTo(record.getIdentity());

      final ResultInternal projectedOther = new ResultInternal(database).setElement(record);
      projectedOther.setProperty("@type", "Other");
      context.setVariable("o", projectedOther);
      assertThat(context.getVariable("$o.@type")).isEqualTo("Other");
      assertThat(context.getVariablePath("$o.@type")).isEqualTo("Other");

      // ORDER BY @TYPE finds the projected @type column, not the record's type: "Other" sorts before "T"
      final OrderByItem upperCase = new OrderByItem();
      upperCase.setRecordAttr("@TYPE");
      assertThat(upperCase.compare(projectedOther, plain, context)).isNegative();
      assertThat(context.getVariable("$o.@TYPE")).isEqualTo("Other");

      // a key stored under the exact spelling wins over the canonical one, even holding null
      final ResultInternal bothSpellings = new ResultInternal(database);
      bothSpellings.setProperty("@TYPE", null);
      bothSpellings.setProperty("@type", "T");
      context.setVariable("b", bothSpellings);
      assertThat(context.getVariable("$b.@TYPE")).isNull();
      assertThat(context.getVariablePath("$b.@TYPE")).isNull();
      assertThat(context.getVariable("$b.@type")).isEqualTo("T");
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

      // case-insensitive as in the projection, on a record and on a row that stores the canonical key
      assertThat(context.getVariable("$rec.@TYPE")).isEqualTo("T");
      assertThat(context.getVariable("$row.@TYPE")).isEqualTo("T");
      assertThat(context.getVariablePath("$row.@Type")).isEqualTo("T");
      final Document vertex = database.iterateType("V", false).next().asDocument();
      final Document edge = database.iterateType("E", false).next().asDocument();
      context.setVariable("v", new ResultInternal(vertex));
      context.setVariable("e", new ResultInternal(edge));
      context.setVariable("doc", vertex);
      assertThat(context.getVariable("$v.@cat")).isEqualTo("v");
      assertThat(context.getVariablePath("$e.@cat")).isEqualTo("e");
      assertThat(context.getVariablePath("$e.@in")).isEqualTo(edge.asEdge().getIn());
      assertThat(context.getVariablePath("$doc.@cat")).isEqualTo("v");
      assertThat(context.getVariablePath("$doc.@rid")).isEqualTo(vertex.getIdentity());
      assertThat(context.getVariablePath("$doc.name")).isEqualTo(vertex.get("name"));
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

  /**
   * Regression for #11392.
   * <p>
   * Issue #11392: PostgreSQL's trailing {@code RETURNING projection} on INSERT, UPDATE and DELETE, what pgjdbc appends for
   * {@code RETURN_GENERATED_KEYS} ({@code RETURNING *} or {@code RETURNING "col"}) and what Hibernate IDENTITY, Spring
   * {@code KeyHolder} and jOOQ rely on. The records as the statement left them for INSERT and UPDATE, as they were before it for
   * DELETE; {@code RETURN} keeps working, and the two cannot be combined.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlReturning extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> database.getSchema().createDocumentType("Ret"));
    }

    @Test
    void insertReturningStarAnswersTheInsertedRecords() {
      database.transaction(() -> {
        final List<Result> rows = rows(database.command("sql", "INSERT INTO Ret SET a = 1, b = 'x' RETURNING *"));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<Integer>getProperty("a")).isEqualTo(1);
        assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("x");
      });
    }

    @Test
    void insertReturningAColumnAnswersItForEveryRow() {
      database.transaction(() -> {
        final List<Result> rows = rows(database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y') RETURNING a, @rid"));
        assertThat(rows).extracting(r -> r.<Integer>getProperty("a")).containsExactly(1, 2);
        assertThat(rows).allSatisfy(r -> assertThat(r.<Object>getProperty("@rid")).isNotNull());
      });
    }

    @Test
    void updateReturningAnswersTheRecordsAsTheUpdateLeftThem() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y')").close();
        final List<Result> rows = rows(database.command("sql", "UPDATE Ret SET b = 'z' WHERE a = 1 RETURNING a, b"));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("z");
      });
    }

    @Test
    void deleteReturningAnswersTheRecordsAsTheyWereBeforeTheDelete() {
      database.transaction(() -> database.command("sql", "INSERT INTO Ret (a, b) VALUES (1, 'x'), (2, 'y')").close());

      database.transaction(() -> {
        final List<Result> rows = rows(database.command("sql", "DELETE FROM Ret WHERE a = 2 RETURNING a, b"));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<Integer>getProperty("a")).isEqualTo(2);
        assertThat(rows.getFirst().<String>getProperty("b")).isEqualTo("y");
      });

      assertThat(database.countType("Ret", false)).isEqualTo(1);
    }

    /** An EXTERNAL property lives in storage the delete removes: RETURNING reads it before, not after. */
    @Test
    void deleteReturningReadsAnExternalPropertyBeforeItsStorageIsGone() {
      database.command("sql", "CREATE PROPERTY Ret.blob STRING (EXTERNAL true)");
      database.transaction(() -> database.command("sql", "INSERT INTO Ret SET a = 9, blob = 'payload'").close());
      // REOPENED, SO THE RECORD IS READ BACK FROM DISK WITH ITS EXTERNAL VALUE NOT LOADED YET
      reopenDatabase();

      database.transaction(() -> {
        final List<Result> rows = rows(database.command("sql", "DELETE FROM Ret WHERE a = 9 RETURNING a, blob"));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("blob")).isEqualTo("payload");
      });
      assertThat(database.countType("Ret", false)).isZero();
    }

    /** A record answered whole - RETURNING @this, RETURN BEFORE - carries its EXTERNAL property too, and its identity. */
    @Test
    void aWholeRecordAnsweredByADeleteKeepsItsExternalProperty() {
      database.command("sql", "CREATE PROPERTY Ret.blob STRING (EXTERNAL true)");
      database.transaction(() -> database.command("sql", "INSERT INTO Ret (a, blob) VALUES (1, 'one'), (2, 'two')").close());
      reopenDatabase();

      for (final String sql : new String[] { "DELETE FROM Ret WHERE a = 1 RETURNING @this", "DELETE FROM Ret RETURN BEFORE WHERE a = 2" })
        database.transaction(() -> {
          final List<Result> rows = rows(database.command("sql", sql));
          assertThat(rows).as(sql).hasSize(1);
          assertThat(rows.getFirst().<String>getProperty("blob")).as(sql).isIn("one", "two");
          assertThat(rows.getFirst().getIdentity()).as(sql).isPresent();
        });
      assertThat(database.countType("Ret", false)).isZero();
    }

    @Test
    void returnAndReturningCannotBeCombined() {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO Ret SET a = 1 RETURN @this RETURNING *"))
          .isInstanceOf(CommandSQLParsingException.class);
      assertThatThrownBy(() -> database.command("sql", "UPDATE Ret SET a = 1 RETURN AFTER RETURNING *"))
          .isInstanceOf(CommandSQLParsingException.class);
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM Ret RETURN BEFORE RETURNING *"))
          .isInstanceOf(CommandSQLParsingException.class);
    }

    /** The keyword stays a name: a property called {@code returning} is written and read as before. */
    @Test
    void returningIsStillAPropertyName() {
      database.transaction(() -> database.command("sql", "INSERT INTO Ret SET returning = 7").close());

      try (final ResultSet rs = database.query("sql", "SELECT returning FROM Ret WHERE returning = 7")) {
        assertThat(rs.next().<Integer>getProperty("returning")).isEqualTo(7);
      }
    }

    /** The statement prints back in a form that parses to the same statement, as every cached plan key relies on. */
    @Test
    void aDeleteReturningPrintsBackToItself() {
      final Statement parsed = parse("DELETE FROM Ret WHERE a = 2 RETURNING a, b");
      assertThat(parsed).isInstanceOf(DeleteStatement.class);

      final StringBuilder printed = new StringBuilder();
      parsed.toString(new HashMap<>(), printed);
      assertThat(printed.toString()).contains("RETURNING").doesNotContain("RETURN BEFORE");
      assertThat(parse(printed.toString())).isEqualTo(parsed);
    }

    private Statement parse(final String sql) {
      return ((DatabaseInternal) database).getStatementCache().get(sql);
    }

    private static List<Result> rows(final ResultSet resultSet) {
      final List<Result> rows = new ArrayList<>();
      try (resultSet) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #11570.
   * <p>
   * Issue #11570: the SQL grammar accepted {@code *} only as the first projection item, so {@code SELECT name, * FROM X} was a
   * syntax error while {@code SELECT *, name FROM X} worked. {@code *} is now accepted at any position of every projection
   * (SELECT, INSERT/UPDATE RETURN, RETURNING). The builder places it first, which is how {@code Projection.toString} has always
   * rendered it, so a statement and its rendering mean the same thing: an explicit item wins over a same-named column of the
   * record wherever the {@code *} is written.
   */
  @Nested
  class ProjectionStarAfterItem extends TestHelper {

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.transaction(() -> database.command("sql", "INSERT INTO P SET name = 'a', secret = 's', n = 1"));
    }

    @Test
    void starAfterAnItemAnswersTheSameRowAsStarFirst() {
      final Result after = single("SELECT name, * FROM P");
      final Result first = single("SELECT *, name FROM P");
      assertThat(after.getPropertyNames()).containsExactlyInAnyOrderElementsOf(first.getPropertyNames());
      assertThat(after.<String>getProperty("name")).isEqualTo("a");
      assertThat(after.<String>getProperty("secret")).isEqualTo("s");
      assertThat(after.<Integer>getProperty("n")).isEqualTo(1);
    }

    @Test
    void starBetweenItems() {
      final Result row = single("SELECT n + 1 AS m, *, name AS alias FROM P");
      assertThat(row.<Integer>getProperty("m")).isEqualTo(2);
      assertThat(row.<String>getProperty("alias")).isEqualTo("a");
      assertThat(row.<String>getProperty("secret")).isEqualTo("s");
    }

    @Test
    void anExplicitItemWinsOverTheStarWhereverTheStarIsWritten() {
      // the same answer as SELECT *, 'x' AS name: the record's own name must not overwrite the explicit item
      assertThat(single("SELECT 'x' AS name, * FROM P").<String>getProperty("name")).isEqualTo("x");
      assertThat(single("SELECT *, 'x' AS name FROM P").<String>getProperty("name")).isEqualTo("x");
    }

    @Test
    void theRenderedStatementParsesBackToTheSameMeaning() {
      final SelectStatement stmt = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get("SELECT 'x' AS name, * FROM P");
      final String rendered = stmt.toString();
      assertThat(rendered).startsWith("SELECT *, ");
      assertThat(single(rendered).<String>getProperty("name")).isEqualTo("x");
    }

    @Test
    void anExclusionBeforeTheStarStillExcludes() {
      final Result row = single("SELECT !secret, * FROM P");
      assertThat(row.getPropertyNames()).doesNotContain("secret");
      assertThat(row.<String>getProperty("secret")).isNull();
      assertThat(row.<String>getProperty("name")).isEqualTo("a");
    }

    @Test
    void distinctWithTheStarAfterAnItem() {
      final Result row = single("SELECT DISTINCT name, * FROM P");
      assertThat(row.<String>getProperty("name")).isEqualTo("a");
      assertThat(row.<String>getProperty("secret")).isEqualTo("s");
    }

    @Test
    void insertReturnAndReturningAcceptTheStarAfterAnItem() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "INSERT INTO P SET name = 'b', n = 2 RETURN n + 10 AS m, *")) {
          final Result row = rs.next();
          assertThat(row.<Integer>getProperty("m")).isEqualTo(12);
          assertThat(row.<String>getProperty("name")).isEqualTo("b");
        }
        try (final ResultSet rs = database.command("sql", "INSERT INTO P SET name = 'c', n = 3 RETURNING n + 10 AS m, *")) {
          final Result row = rs.next();
          assertThat(row.<Integer>getProperty("m")).isEqualTo(13);
          assertThat(row.<String>getProperty("name")).isEqualTo("c");
        }
      });
    }

    @Test
    void updateReturnAndReturningAcceptTheStarAfterAnItem() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "UPDATE P SET n = 5 RETURN AFTER n + 10 AS m, * WHERE name = 'a'")) {
          final Result row = rs.next();
          assertThat(row.<Integer>getProperty("m")).isEqualTo(15);
          assertThat(row.<String>getProperty("secret")).isEqualTo("s");
        }
        try (final ResultSet rs = database.command("sql", "UPDATE P SET n = 6 WHERE name = 'a' RETURNING n + 10 AS m, *")) {
          final Result row = rs.next();
          assertThat(row.<Integer>getProperty("m")).isEqualTo(16);
          assertThat(row.<String>getProperty("secret")).isEqualTo("s");
        }
      });
    }

    @Test
    void deleteReturningAcceptsTheStarAfterAnItem() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "DELETE FROM P WHERE name = 'a' RETURNING n + 10 AS m, *")) {
          final Result row = rs.next();
          assertThat(row.<Integer>getProperty("m")).isEqualTo(11);
          assertThat(row.<String>getProperty("secret")).isEqualTo("s");
        }
      });
    }

    /**
     * The classifier entry {@code SELECT name, * FROM Linked} of {@code MaterializedViewPerRecordClassifierTest} used to be
     * rejected because it did not parse; it is now simple, so its rejection comes from the reserved-name guard, and the same
     * shape over a type without the reserved property is accepted.
     */
    @Test
    void thePerRecordClassifierReachesTheReservedNameGuardForAStarAfterAnItem() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Linked").createProperty("_sourceRID", Type.LINK);
        database.getSchema().createDocumentType("Other");
      });
      assertThat(MaterializedViewQueryClassifier.isSimple("SELECT name, * FROM Linked", database)).isTrue();
      assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name, * FROM Linked", database)).isFalse();
      assertThat(MaterializedViewQueryClassifier.isPerRecord("SELECT name, * FROM Other", database)).isTrue();
    }

    private Result single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        return rs.next();
      }
    }
  }

  /**
   * Regression for #4915.
   * <p>
   * Reproduces issue #4915: map string-key indexing ($map["key"]) returned null inside
   * INSERT ... CONTENT while it worked with INSERT ... SET.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MapBracketNotationInInsert extends TestHelper {
    @Test
    void contentBracketNotationWithStringKey() {
      database.command("sql", "CREATE DOCUMENT TYPE yolo");
      final String script = """
          begin;
          LET $test = {"name":"1","value":"2"};
          LET $inserted = INSERT INTO yolo CONTENT {key:"name",value:$test["name"]};
          commit;
          return $inserted;
          """;
      final ResultSet rs = database.command("sqlscript", script);
      final Result r = rs.next();
      assertThat(r.<String>getProperty("value")).isEqualTo("1");
      assertThat(r.<String>getProperty("key")).isEqualTo("name");
    }

    @Test
    void setBracketNotationWithStringKey() {
      database.command("sql", "CREATE DOCUMENT TYPE yolo");
      final String script = """
          begin;
          LET $test = {"name":"1","value":"2"};
          LET $inserted = INSERT INTO yolo SET key="name",value=$test["name"];
          commit;
          return $inserted;
          """;
      final ResultSet rs = database.command("sqlscript", script);
      final Result r = rs.next();
      assertThat(r.<String>getProperty("value")).isEqualTo("1");
      assertThat(r.<String>getProperty("key")).isEqualTo("name");
    }

    @Test
    void contentNumericIndexStillWorks() {
      database.command("sql", "CREATE DOCUMENT TYPE yolo");
      final String script = """
          begin;
          LET $test = ["a","b","c"];
          LET $inserted = INSERT INTO yolo CONTENT {value:$test[1]};
          commit;
          return $inserted;
          """;
      final ResultSet rs = database.command("sqlscript", script);
      final Result r = rs.next();
      assertThat(r.<String>getProperty("value")).isEqualTo("b");
    }
  }
}
