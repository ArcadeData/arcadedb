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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5756 - the column projection push-down that {@code ScanWithFilterStep} used to carry was unreachable by
 * construction: the planner computed the projected column set only for a statement with no WHERE clause, while the
 * filtered scan exists only because the statement has one. It has been removed.
 * <p>
 * These tests pin the contract a future column push-down would have to honour, because the removed one did not:
 * it emitted a row carrying the projected columns as content <em>and</em> the record as element, and
 * {@link ResultInternal} reads those two stores with different precedences. A column left out of the projected set
 * therefore answered {@code null} to {@code getProperty()} while {@code hasProperty()} still reported {@code true} -
 * so every downstream reader of a column the SELECT does not name (a per-record LET, an ORDER BY alias, a subquery)
 * would have silently read {@code null} instead of the stored value.
 */
class Issue5756FilteredScanColumnPushdownTest extends TestHelper {

  private static final int RECORD_COUNT = 40;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Wide");
      for (int i = 0; i < RECORD_COUNT; i++) {
        final MutableDocument doc = database.newDocument("Wide");
        doc.set("id", i);
        doc.set("name", "n" + i);
        doc.set("status", i % 2 == 0 ? "active" : "closed");
        doc.set("amount", i * 1.5);
        doc.set("note", "note_" + i);
        doc.save();
      }
    });
  }

  /**
   * The filtered scan is chosen exactly when there is a WHERE clause, so no plan can ever advertise a column
   * push-down on it. Before the fix the marker was emitted by dead code that no query shape could reach; after it,
   * the concept is gone. Either way the plan must never claim to project columns at the scan.
   */
  @Test
  void noFilteredScanPlanAdvertisesAColumnPushdown() {
    final List<String> queries = List.of(
        "SELECT name, amount FROM Wide WHERE status = 'closed'",
        "SELECT name FROM Wide WHERE id > 30",
        "SELECT name FROM Wide WHERE id > 30 ORDER BY amount DESC",
        "SELECT name, amount FROM Wide WHERE name IS NOT NULL AND id > 30",
        "SELECT status FROM Wide WHERE id > 2 GROUP BY status",
        "SELECT name, $a AS a FROM Wide LET $a = amount WHERE id > 30");

    for (final String query : queries) {
      final String plan = explain(query);
      assertThat(plan).as("filtered scan expected for: %s", query).contains("SCAN WITH FILTER");
      assertThat(plan).as("no column push-down may be advertised for: %s", query).doesNotContain("[projected:");
    }
  }

  /**
   * A narrow SELECT over a filtered scan returns exactly the requested columns, and the column the filter ran on is
   * not smuggled into the row through any accessor - not {@code getPropertyNames()}, not {@code hasProperty()}, and
   * not the defaulting {@code getProperty(name, default)} overload that reads the backing element.
   */
  @Test
  void narrowProjectionOverAFilteredScanReturnsExactlyTheSelectedColumns() {
    final ResultSet rs = database.query("SQL", "SELECT name, amount FROM Wide WHERE status = 'closed'");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("name", "amount");
      assertThat(row.<String>getProperty("name")).startsWith("n");
      assertThat(row.<Double>getProperty("amount")).isNotNull();

      assertThat(row.hasProperty("status")).isFalse();
      assertThat(row.<Object>getProperty("status")).isNull();
      assertThat(row.<Object>getProperty("status", "RESURRECTED")).isEqualTo("RESURRECTED");
      assertThat(row.toMap()).doesNotContainKeys("status", "note");
      count++;
    }
    assertThat(count).isEqualTo(RECORD_COUNT / 2);
  }

  /**
   * The decisive guard. {@code amount} is read by a per-record LET, downstream of the filtered scan and outside the
   * SELECT list. Under the removed push-down the scan would have emitted a row whose content held only
   * {@code name}, and {@code $a} would have evaluated to null on every row while the query still looked healthy.
   */
  @Test
  void aColumnOutsideTheSelectListIsStillReadableDownstreamOfAFilteredScan() {
    final ResultSet rs = database.query("SQL", "SELECT name, $a AS a FROM Wide LET $a = amount WHERE id > 30");

    final List<Double> amounts = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      final Double a = row.getProperty("a");
      assertThat(a).as("LET over a filtered scan must read the stored value, not null").isNotNull();
      amounts.add(a);
    }
    assertThat(amounts).hasSize(RECORD_COUNT - 31);
    assertThat(amounts).containsExactly(31 * 1.5, 32 * 1.5, 33 * 1.5, 34 * 1.5, 35 * 1.5, 36 * 1.5, 37 * 1.5, 38 * 1.5,
        39 * 1.5);
  }

  /**
   * Same guard on the sort path: {@code note} is neither selected nor filtered on, and the ORDER BY has to see its
   * real values on rows coming out of the filtered scan.
   */
  @Test
  void orderByAColumnOutsideTheSelectListOverAFilteredScan() {
    final ResultSet rs = database.query("SQL", "SELECT name FROM Wide WHERE status = 'active' ORDER BY amount DESC LIMIT 3");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));

    assertThat(names).containsExactly("n38", "n36", "n34");
  }

  /**
   * A record missing one of the projected columns still survives the filter and reports the column as absent rather
   * than as a value borrowed from another record.
   */
  @Test
  void aRecordMissingAProjectedColumnIsReportedAsAbsent() {
    database.transaction(() -> {
      final MutableDocument sparse = database.newDocument("Wide");
      sparse.set("id", 100);
      sparse.set("status", "closed");
      sparse.save();
    });

    final ResultSet rs = database.query("SQL", "SELECT name, amount FROM Wide WHERE id = 100");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Object>getProperty("name")).isNull();
    assertThat(row.<Object>getProperty("amount")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }

  private String explain(final String query) {
    final ResultSet rs = database.query("SQL", "EXPLAIN " + query);
    assertThat(rs.hasNext()).isTrue();
    return rs.next().getProperty("executionPlanAsString");
  }
}
