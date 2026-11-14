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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11730: on a LIST declared without an item type, a BY ITEM index and a scan answer CONTAINS alike when a list holds a
 * nested list. A nested list is an element of its own kind: a scalar operand never matches it (as a list operand never matched a
 * scalar element, #10448), and a one-item list operand matches the element that is that list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UntypedListByItemNestedListTest extends TestHelper {

  private static final List<List<Object>> TAGS = Arrays.asList(//
      Arrays.asList(null, 3, null),//
      Arrays.asList(1, "1", 1.0),//
      List.of(3),//
      List.of("3"),//
      List.of(List.of(3)),//
      List.of(),//
      List.of("[3]"),//
      List.of(Map.of("a", 3)),//
      List.of(List.of(1, 2)));

  @Override
  public void beginTest() {
    for (final String type : List.of("Indexed", "Plain")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tags LIST");
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    }
    database.command("sql", "CREATE INDEX ON Indexed (tags BY ITEM) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : List.of("Indexed", "Plain"))
        for (int k = 0; k < TAGS.size(); k++)
          database.newDocument(type).set("k", k).set("tags", new ArrayList<>(TAGS.get(k))).save();
    });
  }

  @Test
  void scalarOperandDoesNotMatchANestedList() {
    assertSameRows("tags CONTAINS 3", List.of(0, 2, 3));
    assertSameRows("tags CONTAINS '3'", List.of(0, 2, 3));
  }

  @Test
  void textOfANestedListIsNotTheList() {
    // the index keys the nested [3] under its text "[3]", the very key of the string element '[3]'
    assertSameRows("tags CONTAINS '[3]'", List.of(6));
  }

  @Test
  void oneItemListOperandMatchesTheItemAndTheNestedList() {
    // a one-item operand is its item (records 0, 2, 3), or the list itself as one element (record 4)
    assertSameRows("tags CONTAINS [3]", List.of(0, 2, 3, 4));
    assertSameRows("tags CONTAINS [[3]]", List.of(4));
    assertSameRows("tags CONTAINS [[1, 2]]", List.of(8));
  }

  @Test
  void parameterOperandsAgreeToo() {
    assertSameRows("tags CONTAINS :v", Map.of("v", List.of(3)), List.of(0, 2, 3, 4));
    assertSameRows("tags CONTAINS :v", Map.of("v", new int[] { 3 }), List.of(0, 2, 3, 4));
    assertSameRows("tags CONTAINS :v", Map.of("v", 3), List.of(0, 2, 3));
  }

  @Test
  void aNestedListOfRecordLinksIsFoundByAOneItemOperand() {
    final List<Object> rids = new ArrayList<>();
    for (final String type : List.of("Indexed", "Plain"))
      database.transaction(() -> {
        final var target = database.newDocument(type).set("k", 100).save();
        database.newDocument(type).set("k", 101).set("tags", new ArrayList<>(List.of(List.of(target.getIdentity())))).save();
        rids.add(target.getIdentity());
      });
    assertThat(keys("Plain", "tags CONTAINS :v", Map.of("v", List.of(rids.get(1))))).isEqualTo(List.of(101));
    assertThat(keys("Indexed", "tags CONTAINS :v", Map.of("v", List.of(rids.get(0))))).isEqualTo(List.of(101));
  }

  @Test
  void aNestedByItemPathKeepsTheRecheckAndTheWholeOperandLookup() {
    for (final String type : List.of("NIndexed", "NPlain")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tags LIST");
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    }
    database.command("sql", "CREATE INDEX ON NIndexed (`tags.id` BY ITEM) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : List.of("NIndexed", "NPlain")) {
        database.newDocument(type).set("k", 0).set("tags", new ArrayList<>(List.of(Map.of("id", 3)))).save();
        database.newDocument(type).set("k", 1).set("tags", new ArrayList<>(List.of(Map.of("id", List.of(3))))).save();
        database.newDocument(type).set("k", 2).set("tags", new ArrayList<>(List.of(Map.of("id", "[3]")))).save();
      }
    });
    for (final String where : List.of("tags.id CONTAINS 3", "tags.id CONTAINS [3]", "tags.id CONTAINS '[3]'"))
      assertThat(keys("NIndexed", where, Map.of())).as(where).isEqualTo(keys("NPlain", where, Map.of()));
  }

  @Test
  void aNestedByItemPathOverADeclaredItemTypeWithAListField() {
    database.command("sql", "CREATE DOCUMENT TYPE Tag");
    database.command("sql", "CREATE PROPERTY Tag.id LIST");
    for (final String type : List.of("TIndexed", "TPlain")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tags LIST OF Tag");
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    }
    database.command("sql", "CREATE INDEX ON TIndexed (`tags.id` BY ITEM) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : List.of("TIndexed", "TPlain")) {
        database.command("sql", "INSERT INTO " + type + " SET k = 0, tags = [{'@type': 'Tag', 'id': [3]}]");
        database.command("sql", "INSERT INTO " + type + " SET k = 1, tags = [{'@type': 'Tag', 'id': [4]}]");
        database.command("sql", "INSERT INTO " + type + " SET k = 2, tags = [{'@type': 'Tag', 'id': ['[3]']}]");
      }
    });
    for (final String where : List.of("tags.id CONTAINS [3]", "tags.id CONTAINS [[3]]", "tags.id CONTAINS '[3]'")) {
      assertThat(keys("TIndexed", where, Map.of())).as(where).isEqualTo(keys("TPlain", where, Map.of()));
      final String plan = database.query("sql", "EXPLAIN SELECT k FROM TIndexed WHERE " + where).next().getProperty("executionPlan")
          .toString();
      assertThat(plan).as(where).contains("FETCH FROM INDEX TIndexed[tags.idbyitem]");
    }
    // the nested list [3] is found through the whole-operand lookup, which a scalar lookup alone would miss
    assertThat(keys("TIndexed", "tags.id CONTAINS [[3]]", Map.of())).contains(0).doesNotContain(1);
  }

  @Test
  void theIndexIsStillUsed() {
    for (final String where : List.of("tags CONTAINS 3", "tags CONTAINS [3]", "tags CONTAINS '[3]'")) {
      final String plan = database.query("sql", "EXPLAIN SELECT k FROM Indexed WHERE " + where).next().getProperty("executionPlan")
          .toString();
      assertThat(plan).as(where).contains("FETCH FROM INDEX Indexed[tagsbyitem]");
    }
  }

  private void assertSameRows(final String where, final List<Integer> expected) {
    assertSameRows(where, Map.of(), expected);
  }

  private void assertSameRows(final String where, final Map<String, Object> params, final List<Integer> expected) {
    assertThat(keys("Plain", where, params)).as("scan: " + where).isEqualTo(expected);
    assertThat(keys("Indexed", where, params)).as("index: " + where).isEqualTo(expected);
  }

  private List<Integer> keys(final String type, final String where, final Map<String, Object> params) {
    final List<Integer> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM " + type + " WHERE " + where, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        keys.add(row.<Integer>getProperty("k"));
      }
    }
    keys.sort(null);
    return keys;
  }
}
