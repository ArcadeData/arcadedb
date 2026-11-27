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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GeneratedAlias;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code x IN (SELECT ...)} and {@code x NOT IN (SELECT ...)} over a sub-query that does not read the outer row are answered
 * from a membership built once per execution instead of walking every sub-query row for every outer row (issue #12736). These
 * tests pin the answers, which must not move: strings, numbers of different classes, NULL rows under three-valued logic, an
 * empty sub-query, rows of several columns, records and RIDs, rows holding a collection, a sub-query mixing strings and
 * numbers, a bound left operand, and a correlated sub-query, whose rows change from one outer row to the next.
 */
class InSubQueryMembershipIssue12736Test extends TestHelper {

  /**
   * Rows of L besides 'a' and 'c', and the padding of T and SM: with them the sub-query is well past the size below which its
   * rows are scanned (32, {@code InCondition.ROWS_PROBE_MIN_SIZE}).
   */
  private static final int LARGE_PADDING_ROWS = 40;

  @Override
  protected void beginTest() {
    final DocumentType p = database.getSchema().createDocumentType("P");
    p.createProperty("id", Type.INTEGER);
    p.createProperty("name", Type.STRING);
    p.createProperty("num", Type.INTEGER);
    p.createProperty("dbl", Type.DOUBLE);
    p.createProperty("ref", Type.LINK);

    final DocumentType s = database.getSchema().createDocumentType("S");
    s.createProperty("sname", Type.STRING);
    s.createProperty("slong", Type.LONG);
    s.createProperty("sdbl", Type.DOUBLE);
    s.createProperty("sint", Type.INTEGER);

    database.getSchema().createDocumentType("SN").createProperty("sname", Type.STRING);
    database.getSchema().createDocumentType("SM");
    database.getSchema().createDocumentType("T").createProperty("tags", Type.LIST);

    // Enough rows for the sub-query to be probed rather than scanned: S and SN above stay below that size, SM and T are padded
    // past it below, so the probe has to decline their rows (mixed kinds, collections) and leave them to the scan
    final DocumentType l = database.getSchema().createDocumentType("L");
    l.createProperty("lname", Type.STRING);
    l.createProperty("lnull", Type.STRING);
    l.createProperty("llong", Type.LONG);
    l.createProperty("ldbl", Type.DOUBLE);

    database.transaction(() -> {
      for (int i = 0; i < LARGE_PADDING_ROWS; i++)
        database.newDocument("L").set("lname", "pad" + i).set("lnull", i == 7 ? null : "pad" + i).set("llong", 1000L + i)
            .set("ldbl", 1000.5 + i).save();
      database.newDocument("L").set("lname", "a").set("lnull", "a").set("llong", 1L).set("ldbl", 1.0).save();
      database.newDocument("L").set("lname", "c").set("lnull", "c").set("llong", 3L).set("ldbl", 2.5).save();
    });

    database.transaction(() -> {
      final RID s1 = source("a", 1L, 1.0, 2);
      final RID s2 = source("c", 3L, 2.5, 3);
      final RID s3 = source("zz", 99L, 99.0, 100);

      outer(1, "a", 1, 1.0, s1);
      outer(2, "b", 2, 2.5, s2);
      outer(3, "c", 3, 3.0, s3);
      outer(4, "d", 4, 4.0, null);
      outer(5, null, null, null, null);
      outer(6, "e", 5, 5.0, null);

      database.command("sql", "INSERT INTO SN SET sname = 'a'");
      database.command("sql", "INSERT INTO SN SET sname = null");
      database.command("sql", "INSERT INTO SN SET sname = 'c'");

      database.command("sql", "INSERT INTO SM SET v = 'a'");
      database.command("sql", "INSERT INTO SM SET v = 3");

      database.command("sql", "INSERT INTO T SET tags = ['a', 'b']");

      // Padding that matches no outer row: the answers stay those of the rows above
      for (int i = 0; i < LARGE_PADDING_ROWS; i++) {
        database.newDocument("SM").set("v", "pad" + i).save();
        database.newDocument("T").set("tags", List.of("pad" + i, "pad" + (i + 1))).save();
      }
    });
  }

  @Test
  void stringsInAndNotIn() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT sname FROM S)")).containsExactly(1, 3);
    // The NULL name is UNKNOWN against a non-empty sub-query, so NOT IN leaves it out as well
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT sname FROM S)")).containsExactly(2, 4, 6);
  }

  /**
   * The answers of the small sub-queries above, over a sub-query large enough to be answered from the probe: a probe that
   * disagreed with the scan would move them.
   */
  @Test
  void aLargeSubQueryAnswersLikeTheScan() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT lname FROM L)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT lname FROM L)")).containsExactly(2, 4, 6);

    // INTEGER property against LONG rows, and against DOUBLE rows (1.0, 2.5, 1000.5, ...)
    assertThat(ids("SELECT id FROM P WHERE num IN (SELECT llong FROM L)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE num NOT IN (SELECT llong FROM L)")).containsExactly(2, 4, 6);
    assertThat(ids("SELECT id FROM P WHERE num IN (SELECT ldbl FROM L)")).containsExactly(1);
    assertThat(ids("SELECT id FROM P WHERE num NOT IN (SELECT ldbl FROM L)")).containsExactly(2, 3, 4, 6);
    // DOUBLE property against LONG rows (1, 3, 1000, ...)
    assertThat(ids("SELECT id FROM P WHERE dbl IN (SELECT llong FROM L)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE dbl NOT IN (SELECT llong FROM L)")).containsExactly(2, 4, 6);

    // A NULL row makes every miss UNKNOWN
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT lnull FROM L)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT lnull FROM L)")).isEmpty();

    // A bound left operand is converted to the row type, as for "="
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT llong FROM L)", Map.of("v", "3"))).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT llong FROM L)", Map.of("v", 4.0))).isEmpty();
    assertThat(ids("SELECT id FROM P WHERE :v NOT IN (SELECT lname FROM L)", Map.of("v", "q"))).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void numbersOfDifferentClassesCompareByValue() {
    // INTEGER property against LONG rows
    assertThat(ids("SELECT id FROM P WHERE num IN (SELECT slong FROM S)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE num NOT IN (SELECT slong FROM S)")).containsExactly(2, 4, 6);
    // INTEGER property against DOUBLE rows (1.0, 2.5, 99.0)
    assertThat(ids("SELECT id FROM P WHERE num IN (SELECT sdbl FROM S)")).containsExactly(1);
    assertThat(ids("SELECT id FROM P WHERE num NOT IN (SELECT sdbl FROM S)")).containsExactly(2, 3, 4, 6);
    // DOUBLE property against INTEGER rows (2, 3, 100)
    assertThat(ids("SELECT id FROM P WHERE dbl IN (SELECT sint FROM S)")).containsExactly(3);
    assertThat(ids("SELECT id FROM P WHERE dbl NOT IN (SELECT sint FROM S)")).containsExactly(1, 2, 4, 6);
  }

  @Test
  void aNullRowMakesAMissUnknown() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT sname FROM SN)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT sname FROM SN)")).isEmpty();
  }

  @Test
  void anEmptySubQueryMatchesNothingAndExcludesNothing() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT sname FROM S WHERE sname = 'nothing')")).isEmpty();
    // Empty is FALSE even for the NULL name, so its negation keeps every row
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT sname FROM S WHERE sname = 'nothing')"))
        .containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void rowsOfSeveralColumnsCompareThroughTheirFirstColumn() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT sname, slong FROM S)")).containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT sname, slong FROM S)")).containsExactly(2, 4, 6);
  }

  @Test
  void recordsAndRids() {
    assertThat(ids("SELECT id FROM P WHERE @rid IN (SELECT @rid FROM P WHERE num < 3)")).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM P WHERE @rid NOT IN (SELECT @rid FROM P WHERE num < 3)")).containsExactly(3, 4, 5, 6);
    assertThat(ids("SELECT id FROM P WHERE ref IN (SELECT @rid FROM S WHERE sname <> 'zz')")).containsExactly(1, 2);
    // A NULL link is UNKNOWN against a non-empty sub-query
    assertThat(ids("SELECT id FROM P WHERE ref NOT IN (SELECT @rid FROM S WHERE sname <> 'zz')")).containsExactly(3);
    // Whole records as rows
    assertThat(ids("SELECT id FROM P WHERE ref IN (SELECT FROM S WHERE sname = 'a')")).containsExactly(1);
    assertThat(ids("SELECT id FROM P WHERE ref NOT IN (SELECT FROM S WHERE sname = 'a')")).containsExactly(2, 3);
  }

  @Test
  void aRowHoldingACollectionMatchesItsItems() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT tags FROM T)")).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT tags FROM T)")).containsExactly(3, 4, 6);
  }

  @Test
  void aSubQueryMixingStringsAndNumbers() {
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT v FROM SM)")).containsExactly(1);
    assertThat(ids("SELECT id FROM P WHERE num IN (SELECT v FROM SM)")).containsExactly(3);
    assertThat(ids("SELECT id FROM P WHERE num NOT IN (SELECT v FROM SM)")).containsExactly(1, 2, 4, 6);
  }

  @Test
  void aBoundLeftOperandIsConvertedToTheRowType() {
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT sname FROM S)", Map.of("v", "a"))).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT sname FROM S)", Map.of("v", "q"))).isEmpty();
    assertThat(ids("SELECT id FROM P WHERE :v NOT IN (SELECT sname FROM S)", Map.of("v", "q"))).containsExactly(1, 2, 3, 4, 5, 6);
    // A string searched among numbers is converted, as for "="
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT slong FROM S)", Map.of("v", "3"))).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT slong FROM S)", Map.of("v", 3))).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(ids("SELECT id FROM P WHERE :v IN (SELECT slong FROM S)", Map.of("v", 4.0))).isEmpty();
  }

  @Test
  void aCorrelatedSubQueryIsReadForEveryOuterRow() {
    // Row 1 reads ['a'], row 3 reads ['c']: reusing row 1's rows for row 3 would lose it
    assertThat(ids("SELECT id FROM P WHERE name IN (SELECT sname FROM S WHERE slong = $parent.$current.num)"))
        .containsExactly(1, 3);
    assertThat(ids("SELECT id FROM P WHERE name NOT IN (SELECT sname FROM S WHERE slong = $parent.$current.num)"))
        .containsExactly(2, 4, 5, 6);

    // The same over rows past the probe size: every outer row is handed a different list, so the list of row 1 ('a' and the
    // padding) must not answer row 3 ('c' and the padding)
    final String correlated = "(SELECT lname FROM L WHERE llong = $parent.$current.num OR llong >= 1000)";
    assertThat(ids("SELECT id FROM P WHERE name IN " + correlated)).containsExactly(1, 3);
    // Row 5 has a NULL name against a non-empty list: UNKNOWN, so NOT IN leaves it out
    assertThat(ids("SELECT id FROM P WHERE name NOT IN " + correlated)).containsExactly(2, 4, 6);
  }

  @Test
  void aSecondExecutionReadsTheSubQueryAgain() {
    final String sql = "SELECT id FROM P WHERE name IN (SELECT sname FROM S)";
    assertThat(ids(sql)).containsExactly(1, 3);
    database.transaction(() -> database.command("sql", "INSERT INTO S SET sname = 'd'"));
    assertThat(ids(sql)).containsExactly(1, 3, 4);

    // The same over a probed sub-query: the probe of the first execution must not answer the second
    final String large = "SELECT id FROM P WHERE name IN (SELECT lname FROM L)";
    assertThat(ids(large)).containsExactly(1, 3);
    database.transaction(() -> database.command("sql", "INSERT INTO L SET lname = 'e'"));
    assertThat(ids(large)).containsExactly(1, 3, 6);
  }

  /**
   * The rows of an uncorrelated sub-query are read to build the membership, then never again: before the fix every outer row
   * walked all of them. Counted on the rows themselves, so no timing is involved. The list must stay at 32 rows or more: a
   * shorter one is scanned on purpose, since building the probe would cost more than the scan it saves.
   */
  @Test
  void theRowsOfTheSameSubQueryAreNotReadAgainForEveryOuterRow() {
    final int[] reads = new int[1];
    final List<Result> rows = new ArrayList<>();
    for (int i = 0; i < 50; i++)
      // Not the exact ResultInternal class, so every read of the row goes through one of the methods counted here
      rows.add(new ResultInternal(Map.of("sname", "v" + i)) {
        @Override
        public boolean isElement() {
          reads[0]++;
          return super.isElement();
        }

        @Override
        public Set<String> getPropertyNames() {
          reads[0]++;
          return super.getPropertyNames();
        }

        @Override
        public <T> T getProperty(final String name) {
          reads[0]++;
          return super.getProperty(name);
        }
      });

    final String alias = GeneratedAlias.PREFIX + "SUBQUERY$$$_0";
    final InCondition in = new InCondition();
    in.left = new Expression(new Identifier("name"));
    in.rightMathExpression = new BaseExpression(new Identifier(alias));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setVariable(alias, rows);

    assertThat(in.evaluate(new ResultInternal(Map.of("name", "v7")), context)).isTrue();
    assertThat(in.evaluate(new ResultInternal(Map.of("name", "miss")), context)).isFalse();
    final int readsAfterTwoOuterRows = reads[0];
    assertThat(readsAfterTwoOuterRows).as("the first outer rows still read the sub-query rows").isPositive();

    for (int i = 0; i < 100; i++)
      assertThat(in.evaluate(new ResultInternal(Map.of("name", "v" + i)), context)).isEqualTo(i < 50);

    assertThat(reads[0]).as("further outer rows are answered without reading the sub-query rows again")
        .isEqualTo(readsAfterTwoOuterRows);
  }

  private RID source(final String sname, final long slong, final double sdbl, final int sint) {
    final MutableDocument doc = database.newDocument("S");
    doc.set("sname", sname).set("slong", slong).set("sdbl", sdbl).set("sint", sint).save();
    return doc.getIdentity();
  }

  private void outer(final int id, final String name, final Integer num, final Double dbl, final RID ref) {
    database.newDocument("P").set("id", id).set("name", name).set("num", num).set("dbl", dbl).set("ref", ref).save();
  }

  private List<Integer> ids(final String sql) {
    return ids(sql, Map.of());
  }

  private List<Integer> ids(final String sql, final Map<String, Object> params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    ids.sort(null);
    return ids;
  }
}
