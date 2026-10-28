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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A-I3: a snapshot freezes the column definitions its segments were written with. A column redefined after the snapshot
 * was taken (here DATE to DATETIME: both LONG columns, the epoch unit differs) is answered from the records, never decoded with the
 * new definition.
 */
class FacetRetypedColumnTest extends TestHelper {
  private static final String    TYPE  = "Event";
  private static final int       DAYS  = 20;
  private static final int       EACH  = 25;
  private static final LocalDate FIRST = LocalDate.of(2020, 1, 1);

  private static LocalDate asDay(final Object value) {
    return switch (value) {
      case LocalDate date -> date;
      case LocalDateTime dateTime -> dateTime.toLocalDate();
      case Date date -> Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
      default -> throw new AssertionError("Not a date: " + value + " (" + value.getClass().getName() + ")");
    };
  }

  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".day DATE");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (day) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < DAYS * EACH; i++)
        database.newDocument(TYPE).set("day", FIRST.plusDays(i % DAYS)).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  /** Drops the DATE column and creates it again as DATETIME (epoch milliseconds): the stored values stay as they are. */
  private void retypeToDateTime() {
    final String index = database.getSchema().getType(TYPE).getPolymorphicColumnarIndex("day").getName();
    database.command("sql", "DROP INDEX `" + index + "`");
    database.command("sql", "DROP PROPERTY " + TYPE + ".day");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".day DATETIME");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (day) COLUMNAR");
  }

  /**
   * The same for a PROJECTION filter evaluator: it decodes a column with the property type the plan was made with, so a column the
   * snapshot holds with another type is not read; the records answer.
   */
  @Test
  @Timeout(120)
  void aProjectionFilterPlannedBeforeARetypeReadsTheRecords() {
    load();
    final WhereClause filter = ((SelectStatement) new SQLAntlrParser(null).parse(
        "SELECT FROM " + TYPE + " WHERE day < '2020-01-06'")).getWhereClause();
    final FacetFilterEvaluator evaluator = FacetFilterEvaluator.choose(filter, database.getSchema().getType(TYPE),
        (DatabaseInternal) database);
    assertThat(evaluator).isInstanceOf(FacetFilterEvaluator.Projection.class);

    retypeToDateTime();
    FacetOracle.awaitCaughtUp(database);
    final ColumnarSnapshot published = manager().projection(database.getSchema().getType(TYPE).getBuckets(true).getFirst().getFileId())
        .acquire();
    try {
      assertThat(published.publishedColumn("day")).as("the DATETIME column is published").isNotNull();
    } finally {
      published.close();
    }

    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(database);
    try {
      evaluator.open(ctx);
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          evaluator.passes(row.getIdentity().orElseThrow(), row, ctx);
        }
      }
      final FacetFilterEvaluator.Projection projection = (FacetFilterEvaluator.Projection) evaluator;
      assertThat(projection.columnAnswers()).isZero();
      assertThat(projection.recordAnswers()).isEqualTo(DAYS * EACH);
    } finally {
      evaluator.close();
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  @Test
  @Timeout(120)
  void aColumnRetypedAfterTheSnapshotIsAnsweredFromTheRecords() {
    load();

    final List<Bucket> buckets = database.getSchema().getType(TYPE).getBuckets(true);
    final int[] bucketIds = { buckets.getFirst().getFileId() };
    // the snapshot is taken here, with the DATE column (epoch days)
    try (final MatchSet matches = MatchSet.fromProjection((DatabaseInternal) database, bucketIds, 0)) {
      matches.setFacetFilterIndex(new int[] { -1 });
      assertThat(matches.snapshot(bucketIds[0])).isNotNull();

      // the column is redefined as DATETIME (epoch milliseconds) before the facets are computed
      retypeToDateTime();

      final FacetResults results = FacetExecutor.execute((DatabaseInternal) database, matches,
          List.of(new TermsFacetSpec("day", "day", null, null, 100, 1, FacetOrder.COUNT_DESC, null, false, null)), new BasicCommandContext());
      final FacetResult day = results.get("day");
      assertThat(day.source()).isEqualTo(FacetExecutor.SCAN);
      assertThat(results.exact()).isTrue();
      final Map<LocalDate, Long> counts = new HashMap<>();
      for (final FacetBucket bucket : day.buckets())
        if (!bucket.isMissing())
          counts.merge(asDay(bucket.value()), bucket.count(), Long::sum);
      final Map<LocalDate, Long> expected = new HashMap<>();
      for (int i = 0; i < DAYS; i++)
        expected.put(FIRST.plusDays(i), (long) EACH);
      assertThat(counts).isEqualTo(expected);
    }
  }
}
