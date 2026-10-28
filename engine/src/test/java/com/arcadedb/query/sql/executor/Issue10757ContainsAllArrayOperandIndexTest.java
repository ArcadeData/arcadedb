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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10757: over a BY ITEM index, {@code tags CONTAINSALL :p} bound to a multi-element ARRAY answered like CONTAINSANY. The planner
 * declined the index for a multi-element {@code Collection} only, so an {@code int[]} reached the index, whose lookup is the union of
 * the items, and nothing re-checked the condition. The decision also read the parameter at planning time without keeping the plan
 * out of the cache, so a plan made for {@code [1]} answered {@code [1, 2]} with the union too.
 * <p>
 * The contract pinned here: the same statement, over the same rows, answers exactly the same with the index present and absent, for
 * every shape the operand can be bound in, in any order of execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10757ContainsAllArrayOperandIndexTest extends TestHelper {

  private void createTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE Tagged").close();
    database.command("sql", "CREATE PROPERTY Tagged.name STRING").close();
    database.command("sql", "CREATE PROPERTY Tagged.tags LIST OF INTEGER").close();
    database.command("sql", "CREATE INDEX ON Tagged (tags BY ITEM) NOTUNIQUE").close();

    database.command("sql", "CREATE DOCUMENT TYPE Plain").close();
    database.command("sql", "CREATE PROPERTY Plain.name STRING").close();
    database.command("sql", "CREATE PROPERTY Plain.tags LIST OF INTEGER").close();

    database.transaction(() -> {
      for (final String type : List.of("Tagged", "Plain")) {
        database.newDocument(type).set("name", "only1").set("tags", new ArrayList<>(List.of(1))).save();
        database.newDocument(type).set("name", "one_two").set("tags", new ArrayList<>(List.of(1, 2))).save();
        database.newDocument(type).set("name", "only2").set("tags", new ArrayList<>(List.of(2))).save();
        database.newDocument(type).set("name", "two_three_one").set("tags", new ArrayList<>(List.of(2, 3, 1))).save();
        database.newDocument(type).set("name", "empty").set("tags", new ArrayList<>()).save();
        database.newDocument(type).set("name", "missing").save();
      }
    });
  }

  private Set<String> names(final String type, final Object operand) {
    final Set<String> names = new TreeSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE tags CONTAINSALL :p", Map.of("p", operand))) {
      rs.stream().forEach(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  private String plan(final Object operand) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT name FROM Tagged WHERE tags CONTAINSALL :p", Map.of("p", operand))) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** Every shape a client can bind the operand in. */
  private static Map<String, Object> shapesOf(final int... items) {
    final Map<String, Object> shapes = new LinkedHashMap<>();
    final List<Integer> list = new ArrayList<>();
    for (final int item : items)
      list.add(item);
    final Integer[] boxed = list.toArray(new Integer[0]);
    final long[] longs = new long[items.length];
    for (int i = 0; i < items.length; i++)
      longs[i] = items[i];
    shapes.put("int[]", items.clone());
    shapes.put("Integer[]", boxed);
    shapes.put("long[]", longs);
    shapes.put("Object[]", list.toArray());
    shapes.put("List", list);
    shapes.put("LinkedHashSet", new LinkedHashSet<>(list));
    return shapes;
  }

  @Test
  void anArrayOperandAnswersAsTheListDoesAndAsTheUnindexedTypeDoes() {
    createTypes();
    // the array binding runs first, on a fresh database, as in the report
    assertThat(names("Tagged", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(names("Tagged", new Integer[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(names("Plain", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    assertThat(names("Tagged", List.of(1, 2))).containsExactly("one_two", "two_three_one");
  }

  @Test
  void theIndexedAndTheUnindexedTypeAgreeForEveryShapeAndEverySize() {
    createTypes();
    final List<int[]> operands = List.of(new int[] {}, new int[] { 1 }, new int[] { 2 }, new int[] { 1, 2 }, new int[] { 2, 1 },
        new int[] { 1, 2, 3 }, new int[] { 1, 1 }, new int[] { 4 }, new int[] { 1, 4 });
    for (final int[] items : operands)
      for (final Map.Entry<String, Object> shape : shapesOf(items).entrySet())
        assertThat(names("Tagged", shape.getValue())).as("%s %s", shape.getKey(), Arrays.toString(items))
            .isEqualTo(names("Plain", shape.getValue()));
  }

  @Test
  void aPlanMadeForOneItemIsNotReusedForTwo() {
    createTypes();
    // one statement text, executed first with one item (an index-eligible operand) and then with two
    assertThat(names("Tagged", List.of(1))).containsExactly("one_two", "only1", "two_three_one");
    assertThat(names("Tagged", List.of(1, 2))).containsExactly("one_two", "two_three_one");
    assertThat(names("Tagged", new int[] { 1 })).containsExactly("one_two", "only1", "two_three_one");
    assertThat(names("Tagged", new int[] { 1, 2 })).containsExactly("one_two", "two_three_one");
    // and an empty operand, which every collection satisfies, is never answered by the index, whatever ran before it
    assertThat(names("Tagged", List.of())).containsExactly("empty", "one_two", "only1", "only2", "two_three_one");
    assertThat(names("Tagged", new int[0])).containsExactly("empty", "one_two", "only1", "only2", "two_three_one");
  }

  /**
   * A whole list is no key of a BY ITEM index (#10448): an array bound to {@code =} is that list too, so it is answered by the scan
   * with or without the index, whatever binding ran before it.
   */
  @Test
  void anArrayEqualToTheWholeListAnswersAsTheListDoes() {
    createTypes();
    for (final Map.Entry<String, Object> shape : shapesOf(1, 2).entrySet()) {
      final Map<String, Object> params = Map.of("p", shape.getValue());
      for (final String type : List.of("Tagged", "Plain"))
        try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE tags = :p", params)) {
          assertThat(rs.stream().map(r -> r.<String>getProperty("name")).toList()).as("%s %s", type, shape.getKey())
              .containsExactly("one_two");
        }
    }
  }

  @Test
  void aMultiItemOperandUsesTheIndexAndRechecksTheCondition() {
    createTypes();
    final String plan = plan(new int[] { 1, 2 });
    assertThat(plan).contains("FETCH FROM INDEX Tagged[tagsbyitem]");
    assertThat(plan).contains("FILTER ITEMS WHERE");
    assertThat(plan(List.of())).doesNotContain("FETCH FROM INDEX");
  }
}
