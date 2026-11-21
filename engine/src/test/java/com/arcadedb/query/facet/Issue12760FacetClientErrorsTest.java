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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A FACETS histogram or range on a property it cannot bucket, and a histogram above the bucket cap, are the caller's mistake: they are
 * refused as a bad query (HTTP 400), as STATS on a non-numeric property is, never as an execution failure (HTTP 500).
 */
class Issue12760FacetClientErrorsTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE PROPERTY T.dt DATETIME");
    database.command("sql", "CREATE PROPERTY T.i LONG");
    database.command("sql", "CREATE INDEX ON T (s) COLUMNAR");
    database.command("sql", "CREATE INDEX ON T (dt) COLUMNAR");
    database.command("sql", "CREATE INDEX ON T (i) COLUMNAR");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET s = 'a', dt = sysdate(), i = 0");
      database.command("sql", "INSERT INTO T SET s = 'b', dt = sysdate(), i = 5");
      database.command("sql", "INSERT INTO T SET s = 'c', dt = sysdate(), i = " + Long.MAX_VALUE);
    });
    FacetOracle.awaitCaughtUp(database);
  }

  private void facets(final String facets) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM T FACETS " + facets)) {
      rs.getFacets();
    }
  }

  @Test
  void histogramOnStringIsRefusedAsBadQuery() {
    assertThatThrownBy(() -> facets("s HISTOGRAM FIXED 10")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("HISTOGRAM needs a number or date property");
  }

  @Test
  void rangesOnStringIsRefusedAsBadQuery() {
    assertThatThrownBy(() -> facets("s RANGES ([1, 2) AS 'x')")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("RANGES needs a number or date property");
  }

  @Test
  void fixedNumberOnDateTimeIsRefusedAsBadQuery() {
    assertThatThrownBy(() -> facets("dt HISTOGRAM FIXED 1")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("needs a number property");
  }

  @Test
  void tooManyBucketsFromTheDataIsRefusedAsBadQuery() {
    assertThatThrownBy(() -> facets("i HISTOGRAM FIXED 0.000001")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("FACETS i: the histogram would have more than 100000 buckets").hasMessageNotContaining("BOUNDS");
  }

  @Test
  void boundsDoNotLimitTheBucketsOfValuesOutsideThem() {
    assertThatThrownBy(() -> facets("i HISTOGRAM FIXED 1 BOUNDS [0, 10]")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("FACETS i: the histogram would have more than 100000 buckets").hasMessageNotContaining("BOUNDS");
  }
}
