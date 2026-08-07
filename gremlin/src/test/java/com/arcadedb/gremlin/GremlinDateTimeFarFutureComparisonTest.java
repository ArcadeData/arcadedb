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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for <a href="https://github.com/ArcadeData/arcadedb/issues/5625">issue #5625</a>: comparing DATETIME
 * edge properties inside a Gremlin {@code where(startKey, P).by(...).by(...)} predicate (as used to implement a
 * validity-range overlap check) returned the wrong result whenever one of the two operands was a far-future date -
 * a common "no expiration" sentinel such as {@code 2499-12-31 23:59:59.999} or {@code 9999-12-31}.
 * <p>
 * Root cause: TinkerPop 3.8.x's {@code Compare.lt/lte/gt/gte} biPredicates route through
 * {@code org.apache.tinkerpop.gremlin.util.GremlinValueComparator} - a class ArcadeDB supplies under the same
 * fully-qualified name as TinkerPop's own, replacing it on the classpath. Its date comparator always normalises
 * both operands to {@link java.time.temporal.ChronoUnit#NANOS} precision via
 * {@link com.arcadedb.utility.DateUtils#dateTimeToTimestamp(Object, java.time.temporal.ChronoUnit)}, regardless of
 * the property's actual precision. That conversion multiplies epoch-seconds by one billion; for any date beyond
 * roughly the year 2262 the multiplication already saturates to {@code Long.MAX_VALUE}, and until this fix, adding
 * the sub-second nanosecond fraction on top of that saturated value silently overflowed a SECOND time and wrapped
 * around to a large NEGATIVE number - inverting chronological order so the far-future sentinel compared as LESS
 * than an ordinary near-present date.
 */
class GremlinDateTimeFarFutureComparisonTest {
  private static final String DB_PATH = "./target/testGremlinDateTimeFarFutureComparison";

  private ArcadeGraph graph;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    graph = ArcadeGraph.open(DB_PATH);

    final BasicDatabase db = graph.getDatabase();
    db.getSchema().createVertexType("Party");
    final EdgeType ownsLegal = db.getSchema().createEdgeType("OWNS_LEGAL");
    ownsLegal.createProperty("_validFrom", Type.DATETIME);
    ownsLegal.createProperty("_validTo", Type.DATETIME);

    db.transaction(() -> {
      final MutableVertex a = db.newVertex("Party").save();
      final MutableVertex b = db.newVertex("Party").save();

      // An ordinary near-present start date paired with the classic "no expiration" far-future sentinel -
      // exactly the shape reported in issue #5625.
      final MutableEdge openEnded = a.newEdge("OWNS_LEGAL", b);
      openEnded.set("_validFrom", LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0));
      openEnded.set("_validTo", LocalDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000));
      openEnded.save();

      // A second, unrelated edge whose validity window is entirely ordinary (no far-future value), used as a
      // control to confirm normal comparisons are unaffected by the fix.
      final MutableEdge bounded = a.newEdge("OWNS_LEGAL", b);
      bounded.set("_validFrom", LocalDateTime.of(2021, 6, 1, 0, 0, 0, 0));
      bounded.set("_validTo", LocalDateTime.of(2022, 6, 1, 0, 0, 0, 0));
      bounded.save();
    });
  }

  @AfterEach
  void tearDown() {
    if (graph != null)
      graph.drop();
  }

  /**
   * Reproduces the reporter's own isolation probe: on the SAME edge, {@code _validFrom <= _validTo} must always
   * hold when {@code _validFrom} is an ordinary date and {@code _validTo} is the far-future sentinel.
   */
  @Test
  void validFromLessThanOrEqualValidToHoldsForFarFutureSentinel() {
    final ResultSet result = graph.gremlin(
        "g.E().hasLabel('OWNS_LEGAL').as('currentEdge').where('currentEdge', lte('currentEdge')).by('_validFrom').by('_validTo').count()")
        .execute();
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((Long) row.getProperty("result"))
        .as("both edges must satisfy _validFrom <= _validTo, including the one with the far-future sentinel")
        .isEqualTo(2L);
  }

  /**
   * The symmetric half of the reporter's overlap check: {@code _validTo >= _validFrom} on the same edge.
   */
  @Test
  void validToGreaterThanOrEqualValidFromHoldsForFarFutureSentinel() {
    final ResultSet result = graph.gremlin(
        "g.E().hasLabel('OWNS_LEGAL').as('currentEdge').where('currentEdge', gte('currentEdge')).by('_validTo').by('_validFrom').count()")
        .execute();
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((Long) row.getProperty("result")).isEqualTo(2L);
  }

  /**
   * A {@code where(key, P).by().by()} comparison over a projected map - the general shape underlying validity
   * range overlap checks - must also order the far-future sentinel correctly.
   */
  @Test
  void projectedDateTimeComparisonOrdersFarFutureSentinelCorrectly() {
    final ResultSet result = graph.gremlin(
        "g.E().hasLabel('OWNS_LEGAL').project('vf','vt').by('_validFrom').by('_validTo').where('vf', lte('vt')).count()")
        .execute();
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((Long) row.getProperty("result")).isEqualTo(2L);
  }

  /**
   * {@code order()} routes through the same {@code GremlinValueComparator} used by {@code P.lt/lte/gt/gte}, so a
   * far-future sentinel must still sort after every ordinary date instead of appearing first as a wrapped
   * negative timestamp would.
   */
  @Test
  void orderByDateTimeSortsFarFutureSentinelLast() {
    final ResultSet result = graph.gremlin(
        "g.E().hasLabel('OWNS_LEGAL').order().by('_validTo', desc).limit(1).project('vt').by('_validTo')").execute();
    assertThat(result.hasNext()).isTrue();
    final Object latest = result.next().getProperty("vt");
    assertThat(latest).isEqualTo(LocalDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000));
  }
}
