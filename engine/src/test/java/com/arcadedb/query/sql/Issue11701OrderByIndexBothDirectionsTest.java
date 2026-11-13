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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11701: an unbounded ORDER BY on an indexed property with a LIMIT is served from the index in both directions whenever
 * the index can place the null keys where the sort puts them: when the property cannot hold a null (MANDATORY NOTNULL),
 * also on a subtype that inherits it, and when the index holds the null keys (NULL_STRATEGY INDEX). SQL sorts null keys
 * first ascending, openCypher last ascending, so the nulls-first direction of an index that skips null keys still needs
 * a scan of the type to find them.
 */
class Issue11701OrderByIndexBothDirectionsTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Rec");
      database.command("sql", "CREATE PROPERTY Rec.k LONG (MANDATORY true, NOTNULL true)");
      database.command("sql", "CREATE PROPERTY Rec.s STRING (MANDATORY true, NOTNULL true)");
      database.command("sql", "CREATE INDEX ON Rec (k) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Rec (s) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE SubRec EXTENDS Rec");
      // openCypher reads a whole label in key order from a single-property index only, SQL also from a composite one
      database.command("sql", "CREATE VERTEX TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.k STRING (MANDATORY true, NOTNULL true)");
      database.command("sql", "CREATE PROPERTY Comp.s STRING");
      database.command("sql", "CREATE INDEX ON Comp (k, s) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE SubComp EXTENDS Comp");
      database.command("sql", "CREATE VERTEX TYPE Opt");
      database.command("sql", "CREATE PROPERTY Opt.k LONG");
      database.command("sql", "CREATE INDEX ON Opt (k) NOTUNIQUE NULL_STRATEGY INDEX");
      database.command("sql", "CREATE VERTEX TYPE SubOpt EXTENDS Opt");
      for (int i = 0; i < 20; i++) {
        // Duplicate k values across the supertype and the subtype, unique s
        database.command("sql", "CREATE VERTEX Rec SET k = ?, s = ?", i % 7, "r" + (100 + i));
        database.command("sql", "CREATE VERTEX SubRec SET k = ?, s = ?", i % 5, "s" + (100 + i));
        database.command("sql", "CREATE VERTEX Comp SET k = ?, s = ?", "c" + i % 3, "x" + i);
        database.command("sql", "CREATE VERTEX SubComp SET k = ?", "c" + i % 8);
        database.command("sql", "CREATE VERTEX Opt SET k = ?", i % 6);
        database.command("sql", "CREATE VERTEX SubOpt SET k = ?", i % 4);
      }
      // Null and missing keys where the property allows them
      database.command("sql", "CREATE VERTEX Opt SET k = null");
      database.command("sql", "CREATE VERTEX Opt");
      database.command("sql", "CREATE VERTEX SubOpt");
    });
  }

  @Test
  void sqlServesBothDirectionsFromTheIndex() {
    for (final String type : List.of("Rec", "SubRec", "Comp", "SubComp", "Opt", "SubOpt"))
      for (final String direction : List.of("ASC", "DESC")) {
        final String query = "SELECT k FROM " + type + " ORDER BY k " + direction + " SKIP 2 LIMIT 9";
        final ResultSet result = database.query("sql", query);
        final List<Object> keys = keys(result);
        final String plan = result.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
        assertThat(plan).as(query).contains("FETCH FROM INDEX VALUES " + direction).doesNotContain("FETCH FROM BUCKET")
            .doesNotContain("ORDER BY");
        // SQL sorts null keys first ascending, last descending
        assertThat(keys).as(query).isEqualTo(expected(type, "ASC".equals(direction), "ASC".equals(direction), 2, 9));
      }
  }

  @Test
  void cypherServesBothDirectionsFromTheIndex() {
    for (final String type : List.of("Rec", "SubRec", "Opt", "SubOpt"))
      for (final String direction : List.of("ASC", "DESC")) {
        final String query = "MATCH (r:" + type + ") RETURN r.k AS k ORDER BY r.k " + direction + " SKIP 2 LIMIT 9";
        final List<Object> keys = keys(database.query("opencypher", query));
        final String plan = explain(query);
        assertThat(plan).as(query).contains("NodeIndexRangeScan").doesNotContain("NodeByLabelScan").doesNotContain("OrderByStep");
        // openCypher sorts null keys last ascending, first descending
        assertThat(keys).as(query).isEqualTo(expected(type, "ASC".equals(direction), !"ASC".equals(direction), 2, 9));
      }
  }

  private String explain(final String query) {
    try (final ResultSet result = database.query("opencypher", "EXPLAIN " + query)) {
      final Result row = result.next();
      return row.toJSON().toString();
    }
  }

  /** The keys the slow plan (scan and sort) returns, computed from every record of the type and its subtypes. */
  @SuppressWarnings("unchecked")
  private List<Object> expected(final String type, final boolean ascending, final boolean nullsFirst, final int skip,
      final int limit) {
    final List<Object> all = keys(database.query("sql", "SELECT k FROM " + type));
    final List<Object> nulls = all.stream().filter(k -> k == null).toList();
    final Comparator<Object> natural = (a, b) -> ((Comparable<Object>) a).compareTo(b);
    final Comparator<Object> order = ascending ? natural : natural.reversed();
    final List<Object> values = all.stream().filter(k -> k != null).sorted(order).toList();
    final List<Object> sorted = nullsFirst ?
        Stream.concat(nulls.stream(), values.stream()).toList() :
        Stream.concat(values.stream(), nulls.stream()).toList();
    return sorted.subList(skip, Math.min(sorted.size(), skip + limit));
  }

  private static List<Object> keys(final ResultSet result) {
    final List<Object> keys = new ArrayList<>();
    while (result.hasNext()) {
      final Object k = result.next().getProperty("k");
      keys.add(k instanceof Number n ? (Object) n.longValue() : k);
    }
    return keys;
  }
}
