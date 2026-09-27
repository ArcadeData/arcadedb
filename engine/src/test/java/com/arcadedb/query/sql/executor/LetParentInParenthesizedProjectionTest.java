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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8442: a LET subquery whose only {@code $parent} reference sits inside a parenthesized boolean (or a CASE) in
 * its projection must stay a per-record LET, not be hoisted to a global LET evaluated once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LetParentInParenthesizedProjectionTest extends TestHelper {

  @BeforeEach
  void setUpData() {
    database.command("sql", "CREATE DOCUMENT TYPE CacheNode");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO CacheNode SET name = 'Leaf1', flag = true, office = 1");
      database.command("sql", "INSERT INTO CacheNode SET name = 'Leaf2', flag = false, office = 1");
      database.command("sql", "INSERT INTO CacheNode SET name = 'Leaf3', flag = true, office = 2");
      database.command("sql", "INSERT INTO CacheNode SET name = 'Leaf4', flag = true, office = 1");
    });
  }

  @Test
  void parenthesizedBooleanReferencingParentIsPerRecord() {
    final String query = """
        select name, $v[0].v as v from CacheNode
        let $v = (select ($parent.current.flag = true and $parent.current.office = 1) as v)
        where name like 'Leaf%'""";

    assertThat(results(query)).containsEntry("Leaf1", true).containsEntry("Leaf2", false).containsEntry("Leaf3", false)
        .containsEntry("Leaf4", true);
    assertThat(explain(query)).doesNotContain("LET (once)");
  }

  @Test
  void issueReproducerWithLetVariables() {
    final String query = """
        select name, office, flag, $v[0].v as v from CacheNode
        let $flag = flag, $office = office,
            $v = (select ($parent.flag = true and $parent.office = 1) as v)
        where name like 'Leaf%'""";

    // the subquery reads $parent.flag / $parent.office, which resolve per row; the planner must not hoist it
    assertThat(results(query)).containsEntry("Leaf1", true).containsEntry("Leaf2", false).containsEntry("Leaf3", false)
        .containsEntry("Leaf4", true);
    assertThat(explain(query)).doesNotContain("LET (once)");
  }

  @Test
  void caseExpressionReferencingParentIsPerRecord() {
    final String query = """
        select name, $v[0].v as v from CacheNode
        let $v = (select case when $parent.current.flag = true then 'yes' else 'no' end as v)
        where name like 'Leaf%'""";

    assertThat(results(query)).containsEntry("Leaf1", "yes").containsEntry("Leaf2", "no").containsEntry("Leaf3", "yes")
        .containsEntry("Leaf4", "yes");
    assertThat(explain(query)).doesNotContain("LET (once)");
  }

  @Test
  void subqueryWithoutParentStaysGlobal() {
    final String query = """
        select name, $v[0].v as v from CacheNode
        let $v = (select (1 = 1 and 2 = 2) as v)
        where name like 'Leaf%'""";

    assertThat(results(query)).containsEntry("Leaf1", true).containsEntry("Leaf2", true);
    assertThat(explain(query)).contains("LET (once)");
  }

  private Map<String, Object> results(final String query) {
    final Map<String, Object> byName = new HashMap<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        byName.put(r.getProperty("name"), r.getProperty("v"));
      }
    }
    assertThat(byName).hasSize(4);
    return byName;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "explain " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
