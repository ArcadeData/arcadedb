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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7898.
 * <p>
 * Every {@code schema:} catalog LISTING step materialised its whole listing on the first pull and then handed
 * back a {@code ResultSet} over the entire thing, ignoring the {@code nRecords} it was asked for. Both paging
 * steps above it assume a batch is bounded by what they requested, and broke in opposite directions:
 * {@link LimitExecutionStep} counted the rows it delivered but never applied its own cut-off, so {@code LIMIT 2}
 * over six types answered six rows; {@link SkipExecutionStep} discarded the whole batch it received rather than
 * the rows it still owed, so {@code SKIP 1} answered none at all.
 * <p>
 * The listings are asserted together rather than one per test because the shape is shared - a new one of exactly
 * this kind was added for {@code schema:triggers} the same week the counting half was fixed, which is why the
 * paging now lives in a single base class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7898SchemaCatalogPagingTest extends TestHelper {

  private static final int TYPES = 6;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      for (int i = 0; i < TYPES; i++) {
        database.command("sql", "CREATE DOCUMENT TYPE T" + i);
        database.command("sql", "CREATE PROPERTY T" + i + ".name STRING");
        database.command("sql", "CREATE INDEX ON T" + i + " (name) NOTUNIQUE");
        database.command("sql", "CREATE TRIGGER trg" + i + " AFTER CREATE ON TYPE T" + i
            + " EXECUTE SQL \"SELECT 1\"");
      }
    });
  }

  @Test
  void limitBoundsEveryCatalogListing() {
    for (final String catalog : List.of("schema:types", "schema:buckets", "schema:indexes", "schema:triggers")) {
      assertThat(count("SELECT FROM " + catalog)).as(catalog + " without a limit").isGreaterThanOrEqualTo(TYPES);
      assertThat(count("SELECT FROM " + catalog + " LIMIT 2")).as(catalog + " LIMIT 2").isEqualTo(2);
      assertThat(count("SELECT FROM " + catalog + " LIMIT 1")).as(catalog + " LIMIT 1").isEqualTo(1);
      assertThat(count("SELECT name FROM " + catalog + " LIMIT 2")).as(catalog + " projected LIMIT 2").isEqualTo(2);
    }
  }

  @Test
  void skipDropsOnlyTheRowsItOwes() {
    final List<String> all = names("SELECT name FROM schema:types");
    assertThat(all).hasSize(TYPES);

    assertThat(names("SELECT name FROM schema:types SKIP 1")).isEqualTo(all.subList(1, TYPES));
    assertThat(names("SELECT name FROM schema:types SKIP 4")).isEqualTo(all.subList(4, TYPES));
    assertThat(names("SELECT name FROM schema:types SKIP " + TYPES)).isEmpty();
  }

  @Test
  void skipAndLimitComposeOverACatalogListing() {
    final List<String> all = names("SELECT name FROM schema:types");

    assertThat(names("SELECT name FROM schema:types SKIP 1 LIMIT 2")).isEqualTo(all.subList(1, 3));
    assertThat(names("SELECT name FROM schema:types SKIP 4 LIMIT 5")).isEqualTo(all.subList(4, TYPES));
    assertThat(names("SELECT name FROM schema:types LIMIT 3")).isEqualTo(all.subList(0, 3));
  }

  /**
   * A listing is no use paged if paging reshuffles it, and the whole-listing answer must not change either: the
   * cut-off is applied on the way out, so the rows and their order stay what an unpaged read reports.
   */
  @Test
  void pagingPreservesTheListingAndItsOrder() {
    final List<String> all = names("SELECT name FROM schema:types");
    final List<String> paged = new ArrayList<>();
    for (int skip = 0; skip < TYPES; skip++)
      paged.addAll(names("SELECT name FROM schema:types SKIP " + skip + " LIMIT 1"));

    assertThat(paged).isEqualTo(all);
  }

  /** ORDER BY forces the whole listing through a sort before the limit, which must not double-count either. */
  @Test
  void limitStillAppliesUnderAnOrderBy() {
    assertThat(count("SELECT name FROM schema:types ORDER BY name DESC LIMIT 2")).isEqualTo(2);
    assertThat(names("SELECT name FROM schema:types ORDER BY name DESC LIMIT 2"))
        .isEqualTo(List.of("T" + (TYPES - 1), "T" + (TYPES - 2)));
  }

  /** A single-row catalog has no paging to do, but must not start answering nothing. */
  @Test
  void singleRowCatalogsAreUnaffected() {
    assertThat(count("SELECT FROM schema:database")).isEqualTo(1);
    assertThat(count("SELECT FROM schema:database LIMIT 5")).isEqualTo(1);
    assertThat(count("SELECT FROM schema:database SKIP 1")).isZero();
  }

  private int count(final String sql) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private List<String> names(final String sql) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
