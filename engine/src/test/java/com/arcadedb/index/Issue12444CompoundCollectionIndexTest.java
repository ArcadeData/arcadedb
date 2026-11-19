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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12444: a compound index with more than one collection modifier ({@code m1 BY KEY, m2 BY VALUE}) expanded only the
 * last modified property and stored the others as whole values, so the indexed path answered nothing where a scan answered
 * the record. Every BY KEY / BY VALUE property, and a group of BY ITEM properties of one list, is now expanded, and the
 * entries are their cross product: the predicates the planner answers with such an index are independent membership tests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue12444CompoundCollectionIndexTest extends TestHelper {

  private static final String BOTH_MAPS = "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'wanted'";

  @Test
  void reporterScenarioIndexAgreesWithScan() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE FreshMapScan");
      database.command("sql", "CREATE DOCUMENT TYPE FreshMapIndexed");
      // the report leaves out the declarations and the index type, which CREATE INDEX requires
      database.command("sql", "CREATE PROPERTY FreshMapIndexed.m1 MAP");
      database.command("sql", "CREATE PROPERTY FreshMapIndexed.m2 MAP");
      database.command("sql", "CREATE INDEX fresh_idx_map_multi ON FreshMapIndexed (m1 BY KEY, m2 BY VALUE) NOTUNIQUE");
      database.command("sql", "INSERT INTO FreshMapScan SET m1 = {'k1':'v1'}, m2 = {'k2':'wanted'}");
      database.command("sql", "INSERT INTO FreshMapIndexed SET m1 = {'k1':'v1'}, m2 = {'k2':'wanted'}");
    });

    assertThat(count("FreshMapScan", BOTH_MAPS)).isEqualTo(1L);
    assertThat(explain("FreshMapIndexed", BOTH_MAPS)).contains("FETCH FROM INDEX fresh_idx_map_multi");
    assertThat(count("FreshMapIndexed", BOTH_MAPS)).isEqualTo(1L);
    // the first column holds the keys of m1, not m1 as a whole
    assertThat(count("FreshMapIndexed", "m1 CONTAINSKEY 'v1' AND m2 CONTAINSVALUE 'wanted'")).isEqualTo(0L);
    // the report's supplementary query compares the whole map, which the scan answers: the index must not change it
    final String wholeMap = "m1 = {'k1':'v1'} AND m2 CONTAINSVALUE 'wanted'";
    assertThat(count("FreshMapScan", wholeMap)).isEqualTo(1L);
    assertThat(count("FreshMapIndexed", wholeMap)).isEqualTo(1L);
  }

  @Test
  void everyKeyOfOneMapPairsWithEveryValueOfTheOther() {
    createTypeWithIndex("Doc", "m1 MAP", "m2 MAP", "(m1 BY KEY, m2 BY VALUE)");
    database.transaction(() -> database.command("sql",
        "INSERT INTO Doc SET name = 'a', m1 = {'k1':'x','k2':'y'}, m2 = {'p':'v1','q':'v2','r':'v1'}"));

    for (final String key : new String[] { "k1", "k2" })
      for (final String value : new String[] { "v1", "v2" })
        assertThat(count("Doc", "m1 CONTAINSKEY '" + key + "' AND m2 CONTAINSVALUE '" + value + "'")).as(key + "/" + value)
            .isEqualTo(1L);
    assertThat(count("Doc", "m1 CONTAINSKEY 'x' AND m2 CONTAINSVALUE 'v1'")).isEqualTo(0L);
    assertThat(count("Doc", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'p'")).isEqualTo(0L);

    // 2 keys x 2 distinct values, the duplicate value 'v1' collapsed
    assertThat(database.getSchema().getIndexByName("Doc[m1bykey,m2byvalue]").countEntries()).isEqualTo(4L);
  }

  @Test
  void keysAndValuesOfTheSameMap() {
    createTypeWithIndex("Same", "m MAP", null, "(m BY KEY, m BY VALUE)");
    database.transaction(() -> database.command("sql", "INSERT INTO Same SET name = 'a', m = {'a':'x','b':'y'}"));

    // an independent membership test on each side: 'a' is a key and 'y' is a value, although not of the same entry
    assertThat(explain("Same", "m CONTAINSKEY 'a' AND m CONTAINSVALUE 'y'")).contains("FETCH FROM INDEX");
    assertThat(count("Same", "m CONTAINSKEY 'a' AND m CONTAINSVALUE 'y'")).isEqualTo(1L);
    assertThat(count("Same", "m CONTAINSKEY 'a' AND m CONTAINSVALUE 'x'")).isEqualTo(1L);
    assertThat(count("Same", "m CONTAINSKEY 'x' AND m CONTAINSVALUE 'a'")).isEqualTo(0L);
  }

  @Test
  void listItemAndMapKeyInOneIndex() {
    createTypeWithIndex("Mixed", "tags LIST", "m MAP", "(tags BY ITEM, m BY KEY)");
    database.transaction(() -> database.command("sql", "INSERT INTO Mixed SET name = 'a', tags = ['t1','t2'], m = {'k1':1,'k2':2}"));

    assertThat(count("Mixed", "tags CONTAINS 't2' AND m CONTAINSKEY 'k1'")).isEqualTo(1L);
    assertThat(count("Mixed", "tags CONTAINS 't1' AND m CONTAINSKEY 'k2'")).isEqualTo(1L);
    assertThat(count("Mixed", "tags CONTAINS 't3' AND m CONTAINSKEY 'k1'")).isEqualTo(0L);
    assertThat(database.getSchema().getIndexByName("Mixed[tagsbyitem,mbykey]").countEntries()).isEqualTo(4L);
  }

  @Test
  void mapKeyFirstThenListItem() {
    createTypeWithIndex("MixedRev", "m MAP", "tags LIST", "(m BY KEY, tags BY ITEM)");
    database.transaction(() -> database.command("sql", "INSERT INTO MixedRev SET name = 'a', tags = ['t1','t2'], m = {'k1':1}"));

    assertThat(count("MixedRev", "m CONTAINSKEY 'k1' AND tags CONTAINS 't2'")).isEqualTo(1L);
    assertThat(database.getSchema().getIndexByName("MixedRev[mbykey,tagsbyitem]").countEntries()).isEqualTo(2L);
  }

  @Test
  void maintainedOnUpdateAndDelete() {
    createTypeWithIndex("Upd", "m1 MAP", "m2 MAP", "(m1 BY KEY, m2 BY VALUE)");
    database.transaction(() -> database.command("sql", "INSERT INTO Upd SET name = 'a', m1 = {'k1':'x'}, m2 = {'p':'v1'}"));
    final Index index = database.getSchema().getIndexByName("Upd[m1bykey,m2byvalue]");

    // change only the second map: the keys of the first one stay, paired with the new values
    database.transaction(() -> database.command("sql", "UPDATE Upd SET m2 = {'p':'v2','q':'v3'} WHERE name = 'a'"));
    assertThat(count("Upd", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'v1'")).isEqualTo(0L);
    assertThat(count("Upd", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'v3'")).isEqualTo(1L);
    assertThat(index.countEntries()).isEqualTo(2L);

    // change only the first map
    database.transaction(() -> database.command("sql", "UPDATE Upd SET m1 = {'k9':'x'} WHERE name = 'a'"));
    assertThat(count("Upd", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'v2'")).isEqualTo(0L);
    assertThat(count("Upd", "m1 CONTAINSKEY 'k9' AND m2 CONTAINSVALUE 'v2'")).isEqualTo(1L);
    assertThat(index.countEntries()).isEqualTo(2L);

    // two updates in one transaction diff against the snapshot of the first one
    database.transaction(() -> {
      database.command("sql", "UPDATE Upd SET m2 = {'z':'w1'} WHERE name = 'a'");
      database.command("sql", "UPDATE Upd SET m1 = {'k5':'x','k6':'y'} WHERE name = 'a'");
    });
    assertThat(count("Upd", "m1 CONTAINSKEY 'k6' AND m2 CONTAINSVALUE 'w1'")).isEqualTo(1L);
    assertThat(count("Upd", "m1 CONTAINSKEY 'k9' AND m2 CONTAINSVALUE 'w1'")).isEqualTo(0L);
    assertThat(index.countEntries()).isEqualTo(2L);

    // an empty map leaves no entry for the record
    database.transaction(() -> database.command("sql", "UPDATE Upd SET m1 = {} WHERE name = 'a'"));
    assertThat(index.countEntries()).isEqualTo(0L);

    database.transaction(() -> database.command("sql", "UPDATE Upd SET m1 = {'k1':'x'} WHERE name = 'a'"));
    assertThat(index.countEntries()).isEqualTo(1L);
    database.transaction(() -> database.command("sql", "DELETE FROM Upd WHERE name = 'a'"));
    assertThat(index.countEntries()).isEqualTo(0L);
    assertThat(count("Upd", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'w1'")).isEqualTo(0L);
  }

  @Test
  void conditionOnTheFirstColumnAloneAgreesWithAScan() {
    createTypeWithIndex("Prefix", "m1 MAP", "m2 MAP", "(m1 BY KEY, m2 BY VALUE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Prefix SET name = 'full', m1 = {'k1':'x'}, m2 = {'p':'v1'}");
      database.command("sql", "INSERT INTO Prefix SET name = 'empty', m1 = {'k1':'x'}, m2 = {}");
      database.command("sql", "INSERT INTO Prefix SET name = 'missing', m1 = {'k1':'x'}");
    });

    // a record with no element in the second map has no entry: an index lookup on the first column alone would miss it
    assertThat(names("Prefix", "m1 CONTAINSKEY 'k1'")).containsExactlyInAnyOrder("full", "empty", "missing");
    // both columns bound: the index answers, and only the record holding both
    assertThat(explain("Prefix", BOTH_MAPS.replace("wanted", "v1"))).contains("FETCH FROM INDEX");
    assertThat(names("Prefix", "m1 CONTAINSKEY 'k1' AND m2 CONTAINSVALUE 'v1'")).containsExactly("full");
  }

  @Test
  void scalarPrefixOfAListItemIndexAgreesWithAScan() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tagged");
      database.command("sql", "CREATE PROPERTY Tagged.name STRING");
      database.command("sql", "CREATE PROPERTY Tagged.tags LIST OF STRING");
      database.command("sql", "CREATE INDEX ON Tagged (name, tags BY ITEM) NOTUNIQUE");
      database.command("sql", "INSERT INTO Tagged SET name = 'x', tags = ['a']");
      database.command("sql", "INSERT INTO Tagged SET name = 'x', tags = []");
    });

    assertThat(count("Tagged", "name = 'x'")).isEqualTo(2L);
    assertThat(explain("Tagged", "name = 'x' AND tags CONTAINS 'a'")).contains("FETCH FROM INDEX");
    assertThat(count("Tagged", "name = 'x' AND tags CONTAINS 'a'")).isEqualTo(1L);
  }

  @Test
  void builtOverExistingData() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Pre");
      database.command("sql", "CREATE PROPERTY Pre.m1 MAP");
      database.command("sql", "CREATE PROPERTY Pre.m2 MAP");
      database.command("sql", "INSERT INTO Pre SET m1 = {'k1':'v1','k2':'v2'}, m2 = {'a':'wanted'}");
      database.command("sql", "INSERT INTO Pre SET m1 = {'k3':'v1'}, m2 = {'a':'wanted'}");
    });
    database.command("sql", "CREATE INDEX ON Pre (m1 BY KEY, m2 BY VALUE) NOTUNIQUE");

    assertThat(database.getSchema().getIndexByName("Pre[m1bykey,m2byvalue]").countEntries()).isEqualTo(3L);
    assertThat(count("Pre", "m1 CONTAINSKEY 'k2' AND m2 CONTAINSVALUE 'wanted'")).isEqualTo(1L);
    assertThat(count("Pre", "m1 CONTAINSKEY 'k3' AND m2 CONTAINSVALUE 'wanted'")).isEqualTo(1L);
  }

  private void createTypeWithIndex(final String type, final String property1, final String property2, final String index) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + "." + property1);
      if (property2 != null)
        database.command("sql", "CREATE PROPERTY " + type + "." + property2);
      database.command("sql", "CREATE INDEX ON " + type + " " + index + " NOTUNIQUE");
    });
  }

  private long count(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE " + where)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<String> names(final String type, final String where) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE " + where)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private String explain(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT count(*) AS c FROM " + type + " WHERE " + where)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
