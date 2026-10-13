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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9911: in a MATCH whose RETURN builds new rows (projections, {@code $paths}, {@code $patterns}), a bare
 * {@code ORDER BY @rid} sorted on a key that is null for every row, so the stable sort silently kept the pattern order. It is
 * now refused at planning time, while {@code ORDER BY <alias>.@rid}, {@code ORDER BY <projected column>} and
 * {@code ORDER BY @rid} over rows that are records ({@code $elements}) keep working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9911MatchOrderByRecordAttributeTest extends TestHelper {
  private static final int COUNT = 15;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE R");
      for (int i = 0; i < COUNT; i++) {
        database.command("sql", "CREATE VERTEX S SET n = " + i);
        database.command("sql", "CREATE VERTEX P SET n = " + i);
        database.command("sql", "CREATE EDGE R FROM (SELECT FROM S WHERE n = " + i + ") TO (SELECT FROM P WHERE n = " + i + ")");
      }
    });
  }

  @Test
  void bareRidOnAProjectionIsRefused() {
    for (final String direction : new String[] { "", " ASC", " DESC" })
      assertThatThrownBy(() -> database.query("sql",
          "MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY @rid" + direction).close())//
          .isInstanceOf(CommandExecutionException.class)//
          .hasMessageContaining("ORDER BY @rid has no meaning on the rows of this MATCH")//
          .hasMessageContaining("<alias>.@rid");
  }

  @Test
  void bareTypeOnAProjectionIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s} RETURN s.n AS n ORDER BY @type").close())//
        .isInstanceOf(CommandExecutionException.class)//
        .hasMessageContaining("ORDER BY @type has no meaning");
  }

  @Test
  void bareRidOnPathsAndPatternsIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s}.out('R'){as:p} RETURN $paths ORDER BY @rid").close())//
        .isInstanceOf(CommandExecutionException.class);
    assertThatThrownBy(() -> database.query("sql", "MATCH {type:S, as:s}.out('R'){as:p} RETURN $patterns ORDER BY @rid").close())//
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void aliasQualifiedRidSortsInBothDirections() {
    final List<RID> expected = allSRids();
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY s.@rid ASC", "srid"))
        .containsExactlyElementsOf(expected);
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY s.@rid DESC", "srid"))
        .containsExactlyElementsOf(expected.reversed());
  }

  @Test
  void projectedColumnSortsInBothDirections() {
    final List<RID> expected = allSRids();
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY srid ASC", "srid"))
        .containsExactlyElementsOf(expected);
    assertThat(ridColumn("MATCH {type:S, as:s}.out('R'){as:p} RETURN s.@rid AS srid, p.n AS pn ORDER BY srid DESC", "srid"))
        .containsExactlyElementsOf(expected.reversed());
  }

  @Test
  void bareRidOnElementsStillSorts() {
    final List<RID> expected = allSRids();
    final List<RID> actual = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "MATCH {type:S, as:s} RETURN $elements ORDER BY @rid DESC")) {
      while (rs.hasNext())
        actual.add(rs.next().getIdentity().get());
    }
    assertThat(actual).containsExactlyElementsOf(expected.reversed());
  }

  private List<RID> allSRids() {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM S")) {
      while (rs.hasNext())
        rids.add(rs.next().getProperty("r"));
    }
    rids.sort(Comparator.naturalOrder());
    assertThat(rids).hasSize(COUNT);
    return rids;
  }

  private List<RID> ridColumn(final String query, final String column) {
    final List<RID> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }
}
