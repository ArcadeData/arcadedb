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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The evaluator of a facet FILTER: which kind the planner picks, and that every kind answers exactly what the same condition as a WHERE
 * answers, record by record, whatever the projection's state.
 */
class FacetFilterEvaluatorTest extends TestHelper {
  private static final String TYPE    = ColumnarTestData.TYPE;
  private static final int    RECORDS = 2_000;

  private ColumnarTestData load() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagVocabulary = 500;
    data.createSchema(database);
    data.insertAssets(database, RECORDS);
    FacetOracle.awaitCaughtUp(database);
    return data;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private DocumentType type() {
    return database.getSchema().getType(TYPE);
  }

  private static WhereClause filter(final String condition) {
    return ((SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM " + TYPE + " WHERE " + condition)).getWhereClause();
  }

  private FacetFilterEvaluator choose(final String condition) {
    return FacetFilterEvaluator.choose(filter(condition), type(), (DatabaseInternal) database);
  }

  private CommandContext context() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(database);
    return ctx;
  }

  private Set<RID> where(final String condition) {
    final Set<RID> rids = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " WHERE " + condition)) {
      while (rs.hasNext())
        rids.add(rs.next().getProperty("rid"));
    }
    return rids;
  }

  /**
   * Opens the evaluator and asserts that it passes exactly the records the condition as a WHERE matches, in the caller's transaction
   * if one is open; both answers must occur. Closes the evaluator.
   */
  private void assertAgreesWithWhere(final FacetFilterEvaluator evaluator, final String condition) {
    final Set<RID> expected = where(condition);
    assertThat(expected).as("records matching " + condition).isNotEmpty();
    final CommandContext ctx = context();
    long passed = 0;
    long total = 0;
    try {
      evaluator.open(ctx);
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final RID rid = row.getIdentity().orElseThrow();
          final boolean passes = evaluator.passes(rid, row, ctx);
          assertThat(passes).as(evaluator.kind() + " " + condition + " on " + rid).isEqualTo(expected.contains(rid));
          if (passes)
            passed++;
          total++;
        }
      }
    } finally {
      evaluator.close();
    }
    assertThat(passed).isEqualTo(expected.size());
    assertThat(passed).as("records refused by " + condition).isLessThan(total);
  }

  @Test
  @Timeout(120)
  void choosesIndexForLsmEquality() {
    load();
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) NOTUNIQUE");

    for (final String condition : List.of("brand IN ['brand-1', 'brand-2']", "brand = 'brand-3'")) {
      final FacetFilterEvaluator evaluator = choose(condition);
      assertThat(evaluator).isInstanceOf(FacetFilterEvaluator.Index.class);
      assertThat(evaluator.kind()).isEqualTo("INDEX");
      assertAgreesWithWhere(evaluator, condition);
    }
    // a range is not looked up in the key index: the COLUMNAR column answers it
    assertThat(choose("brand >= 'brand-5'").kind()).isEqualTo("PROJECTION");
  }

  @Test
  @Timeout(120)
  void choosesProjectionForColumnarRange() {
    load();
    assertThat(choose("fileSize >= 10 AND fileSize < 20").kind()).isEqualTo("PROJECTION");
    assertThat(choose("fileSize >= 10 AND fileSize < 20")).isInstanceOf(FacetFilterEvaluator.Projection.class);

    final long someSize;
    try (final ResultSet rs = database.query("sql", "SELECT fileSize FROM " + TYPE + " LIMIT 1")) {
      someSize = rs.next().<Long>getProperty("fileSize");
    }
    for (final String condition : List.of("fileSize >= 1048576 AND fileSize < 2097152", "fileSize BETWEEN 1024 AND 524288",
        "fileSize IN [0, 1024, 2048, 3072, 4096, " + someSize + "]", "fileSize = " + someSize, "publishDate >= '2022-06-01'",
        "publishDate < '2021-01-01 00:00:00'", "brand IN ['brand-1', 'brand-2']", "brand >= 'brand-5'")) {
      final FacetFilterEvaluator.Projection evaluator = (FacetFilterEvaluator.Projection) choose(condition);
      assertAgreesWithWhere(evaluator, condition);
      // caught up and every record has the property: no record is consulted
      assertThat(evaluator.columnAnswers()).as(condition).isEqualTo(RECORDS);
      assertThat(evaluator.recordAnswers()).as(condition).isZero();
    }

    // a record without the property is answered from the record: the column cannot tell a missing value from one it could not hold
    final String author = "author IN ['author-1', 'author-2', 'author-3']";
    final FacetFilterEvaluator.Projection evaluator = (FacetFilterEvaluator.Projection) choose(author);
    assertAgreesWithWhere(evaluator, author);
    assertThat(evaluator.recordAnswers()).isEqualTo(RECORDS / 10);
    assertThat(evaluator.columnAnswers()).isEqualTo(RECORDS - RECORDS / 10);
  }

  @Test
  @Timeout(120)
  void fallsBackToRecord() {
    load();
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) NOTUNIQUE");
    for (final String condition : List.of("brand.toLowerCase() = 'brand-1'", "fileSize < 1048576 OR brand = 'brand-1'",
        "fileSize + 0 < 1048576", "NOT (fileSize < 1048576)", "tags CONTAINS 'tag-1'", "fileSize < 1048576 AND brand = 'brand-1'")) {
      final FacetFilterEvaluator evaluator = choose(condition);
      assertThat(evaluator.kind()).as(condition).isEqualTo("RECORD");
      assertThat(evaluator).isInstanceOf(FacetFilterEvaluator.Record.class);
      assertAgreesWithWhere(evaluator, condition);
    }
    // no type to plan against (a subquery as target)
    assertThat(FacetFilterEvaluator.choose(filter("brand = 'brand-1'"), null, (DatabaseInternal) database).kind()).isEqualTo("RECORD");
  }

  @Test
  @Timeout(120)
  void projectionPendingPositionReadsRecord() throws Exception {
    load();
    final String condition = "fileSize < 1048576";
    final RID small = where(condition).iterator().next();
    final RID big = where("fileSize >= 1048576").iterator().next();

    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        database.lookupByRID(small, true).asDocument().modify().set("fileSize", 3_000_000L).save();
        database.lookupByRID(big, true).asDocument().modify().set("fileSize", 5L).save();
      });
      final FacetFilterEvaluator.Projection evaluator = (FacetFilterEvaluator.Projection) choose(condition);
      // the column still holds the committed values of before: only the records can answer these two
      assertAgreesWithWhere(evaluator, condition);
      assertThat(where(condition)).contains(big).doesNotContain(small);
      assertThat(evaluator.recordAnswers()).isGreaterThanOrEqualTo(2);
      assertThat(evaluator.columnAnswers()).isPositive();
    }
  }

  @Test
  @Timeout(120)
  void openTransactionWritesAreSeen() {
    load();
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) NOTUNIQUE");
    final RID toBrand1 = where("brand = 'brand-2'").iterator().next();
    final RID fromBrand1 = where("brand = 'brand-1'").iterator().next();
    final RID toSmall = where("fileSize >= 1048576").iterator().next();

    database.begin();
    try {
      database.lookupByRID(toBrand1, true).asDocument().modify().set("brand", "brand-1").save();
      database.lookupByRID(fromBrand1, true).asDocument().modify().set("brand", "brand-9").save();
      database.lookupByRID(toSmall, true).asDocument().modify().set("fileSize", 7L).save();
      database.newDocument(TYPE).set("brand", "brand-1").set("fileSize", 9L).save();

      assertThat(where("brand = 'brand-1'")).contains(toBrand1).doesNotContain(fromBrand1);
      assertAgreesWithWhere(choose("brand = 'brand-1'"), "brand = 'brand-1'");
      assertAgreesWithWhere(choose("fileSize < 1048576"), "fileSize < 1048576");
    } finally {
      database.rollback();
    }
  }

  @Test
  @Timeout(120)
  void dateColumnUnderProjection() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagVocabulary = 500;
    data.dateColumn = true;
    data.createSchema(database);
    data.insertAssets(database, RECORDS);
    FacetOracle.awaitCaughtUp(database);

    final Object someDay;
    try (final ResultSet rs = database.query("sql", "SELECT day FROM " + TYPE + " LIMIT 1")) {
      someDay = rs.next().getProperty("day");
    }
    for (final String condition : List.of("day >= '2022-06-01'", "day < '2021-03-01'", "day BETWEEN '2021-01-01' AND '2021-12-31'",
        "day = '" + someDay + "'", "day IN ['" + someDay + "', '2020-01-01']")) {
      final FacetFilterEvaluator.Projection evaluator = (FacetFilterEvaluator.Projection) choose(condition);
      assertAgreesWithWhere(evaluator, condition);
      assertThat(evaluator.columnAnswers()).as(condition).isEqualTo(RECORDS);
      assertThat(evaluator.recordAnswers()).as(condition).isZero();
    }
  }

  @Test
  @Timeout(120)
  void zeroDoubleIsAnsweredFromTheRecord() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagVocabulary = 500;
    data.doubleColumn = true;
    data.createSchema(database);
    data.insertAssets(database, RECORDS);
    final RID negative = where("score > 50").iterator().next();
    final RID positive = where("score < 50").iterator().next();
    database.transaction(() -> {
      database.lookupByRID(negative, true).asDocument().modify().set("score", -0.0d).save();
      database.lookupByRID(positive, true).asDocument().modify().set("score", 0.0d).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final int zeros = where("score = 0").size();
    assertThat(zeros).isGreaterThanOrEqualTo(2);
    for (final String condition : List.of("score <= 0", "score > 0", "score >= 0 AND score < 1")) {
      final FacetFilterEvaluator.Projection evaluator = (FacetFilterEvaluator.Projection) choose(condition);
      assertAgreesWithWhere(evaluator, condition);
      // the column stores -0.0 as 0.0: every zero is read from its record, nothing else is
      assertThat(evaluator.recordAnswers()).as(condition).isEqualTo(zeros);
    }
  }

  @Test
  @Timeout(120)
  void indexAnswersUncoveredBucketFromRow() {
    load();
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    final RID[] others = new RID[2];
    database.transaction(() -> {
      others[0] = database.newDocument("Other").set("brand", "brand-1").save().getIdentity();
      others[1] = database.newDocument("Other").set("brand", "brand-x").save().getIdentity();
    });
    final FacetFilterEvaluator evaluator = choose("brand = 'brand-1'");
    assertThat(evaluator.kind()).isEqualTo("INDEX");
    final CommandContext ctx = context();
    evaluator.open(ctx);
    try {
      // a bucket the index has no sub-index for: the RID set cannot hold its records, the row decides
      assertThat(evaluator.passes(others[0], new ResultInternal(database.lookupByRID(others[0], true)), ctx)).isTrue();
      assertThat(evaluator.passes(others[1], new ResultInternal(database.lookupByRID(others[1], true)), ctx)).isFalse();
      // no row: the record is loaded by its RID
      assertThat(evaluator.passes(others[0], null, ctx)).isTrue();
    } finally {
      evaluator.close();
    }
  }

  @Test
  @Timeout(120)
  void indexKeyOutOfThePropertyRangeIsNoMatch() {
    database.command("sql", "CREATE DOCUMENT TYPE Num");
    database.command("sql", "CREATE PROPERTY Num.n INTEGER");
    database.command("sql", "CREATE PROPERTY Num.s SHORT");
    database.command("sql", "CREATE INDEX ON Num (n) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Num (s) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Num").set("n", i).set("s", (short) i).save();
      // what the out-of-range keys wrap to: a narrowing key would wrongly find these
      database.newDocument("Num").set("n", (int) 3_000_000_000L).set("s", (short) 40_000).save();
      database.newDocument("Num").set("n", 1).set("s", (short) -40_000).save();
    });
    final DocumentType num = database.getSchema().getType("Num");
    for (final String condition : List.of("n = 3000000000", "n IN [1, 3000000000, 7]", "s = 40000", "s IN [-40000, 2]", "n = 5")) {
      final WhereClause where = filter(condition);
      final FacetFilterEvaluator evaluator = FacetFilterEvaluator.choose(where, num, (DatabaseInternal) database);
      assertThat(evaluator.kind()).as(condition).isEqualTo("INDEX");
      final CommandContext ctx = context();
      evaluator.open(ctx);
      try (final ResultSet rs = database.query("sql", "SELECT FROM Num")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          // the row's own answer: a key no value of the property can hold matches nothing, it does not fail the query
          assertThat(evaluator.passes(row.getIdentity().orElseThrow(), row, ctx)).as(condition + " on " + row.toJSON())
              .isEqualTo(where.matchesFilters(row, ctx));
        }
      } finally {
        evaluator.close();
      }
    }
  }

  @Test
  @Timeout(120)
  void closeReleasesEverySnapshot() {
    load();
    final int[] buckets = new int[type().getBuckets(true).size()];
    for (int i = 0; i < buckets.length; i++)
      buckets[i] = type().getBuckets(true).get(i).getFileId();
    final int before = ColumnarTestAccess.stateReferences(manager(), buckets[0]);

    final FacetFilterEvaluator evaluator = choose("fileSize < 1048576");
    final CommandContext ctx = context();
    evaluator.open(ctx);
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " LIMIT 1")) {
      final Result row = rs.next();
      evaluator.passes(row.getIdentity().orElseThrow(), row, ctx);
    }
    assertThat(ColumnarTestAccess.stateReferences(manager(), buckets[0])).isEqualTo(before + 1);
    evaluator.close();
    assertThat(ColumnarTestAccess.stateReferences(manager(), buckets[0])).isEqualTo(before);
    // idempotent
    evaluator.close();
    assertThat(ColumnarTestAccess.stateReferences(manager(), buckets[0])).isEqualTo(before);
  }
}
