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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12914: {@code UPDATE ... REMOVE l[a..b]} on a LIST reported the record updated and removed nothing (the range start was
 * handed to {@code List.remove(Object)}, which looked for an element EQUAL to it), and {@code REMOVE l[9]} past the end or
 * {@code REMOVE l[-1]} answered an IndexOutOfBoundsException. A REMOVE takes out what the same selector reads: a range the
 * elements {@code l[a..b]} reads (end exclusive, {@code ...} inclusive), and an index outside the list, negative included,
 * reads null, so it selects nothing and removes nothing. The read form had to change for the rule to hold: a range starting
 * exactly at the size read the last element, and an inclusive end of {@code Integer.MAX_VALUE} wrapped and read nothing.
 */
class Issue12914UpdateRemoveListIndexTest extends TestHelper {
  private static final String       TYPE     = "Doc12914";
  private static final List<Object> ORIGINAL = List.of(10, 20, 30, 40);

  static Stream<Arguments> removals() {
    return Stream.of( //
        Arguments.of("l[0..1]", List.of(20, 30, 40)), //
        Arguments.of("l[1..3]", List.of(10, 40)), //
        Arguments.of("l[1...2]", List.of(10, 40)), //
        Arguments.of("l[2..9]", List.of(10, 20)), //
        Arguments.of("l[0..4]", List.of()), //
        Arguments.of("l[3..3]", ORIGINAL), //
        Arguments.of("l[5..7]", ORIGINAL), //
        // a range starting exactly at the size selects nothing: the read used to clamp the start to the last element
        Arguments.of("l[4..6]", ORIGINAL), //
        Arguments.of("l[4...4]", ORIGINAL), //
        Arguments.of("l[-3..2]", List.of(30, 40)), //
        // an inclusive end at Integer.MAX_VALUE: the +1 used to wrap to a negative end, and both forms selected nothing
        Arguments.of("l[1...2147483647]", List.of(10)), //
        Arguments.of("l[0]", List.of(20, 30, 40)), //
        Arguments.of("l[3]", List.of(10, 20, 30)), //
        Arguments.of("l[0,2]", List.of(20, 40)), //
        Arguments.of("l[4]", ORIGINAL), //
        Arguments.of("l[9]", ORIGINAL), //
        Arguments.of("l[-1]", ORIGINAL), //
        Arguments.of("l[0,9]", List.of(20, 30, 40)), //
        // the same index twice names one element: it used to remove the element at that index and then its successor
        Arguments.of("l[1,1]", List.of(10, 30, 40)), //
        Arguments.of("l[-1,1]", List.of(10, 30, 40)));
  }

  @ParameterizedTest(name = "REMOVE {0}")
  @MethodSource("removals")
  void removeTakesOutWhatTheSelectorReads(final String selector, final List<Object> expected) {
    database.getSchema().createDocumentType(TYPE);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("l", new ArrayList<>(ORIGINAL)).save().getIdentity());

    // what the read form selects is what the REMOVE takes out
    final List<Object> selected = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT " + selector + " AS r FROM " + rid[0])) {
      final Object read = rs.next().getProperty("r");
      if (read instanceof Collection<?> items)
        selected.addAll(items);
      else if (read != null)
        selected.add(read);
    }
    // by value, which is exact only because the elements of ORIGINAL are distinct
    final List<Object> remaining = new ArrayList<>(ORIGINAL);
    remaining.removeAll(selected);
    assertThat(remaining).as("SELECT %s", selector).containsExactlyElementsOf(expected);

    database.transaction(() -> database.command("sql", "UPDATE " + rid[0] + " REMOVE " + selector).close());

    assertThat((List<Object>) database.lookupByRID(rid[0], true).asDocument().get("l")).as("REMOVE %s", selector)
        .containsExactlyElementsOf(expected);
  }

  /**
   * The read form on its own, on a literal list and on a property: a range starting at the size reads nothing (it read the last
   * element), one starting just before it still reads that element, and an inclusive end at Integer.MAX_VALUE reads to the end
   * (it read nothing).
   */
  @Test
  void rangeReadAtTheBoundaries() {
    database.getSchema().createDocumentType(TYPE);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("l", new ArrayList<>(ORIGINAL)).save().getIdentity());

    try (final ResultSet rs = database.query("sql",
        "SELECT [10,20,30,40][4..6] AS lit, l[4..6] AS atSize, l[3..6] AS last, l[1...2147483647] AS toMax FROM " + rid[0])) {
      final Result row = rs.next();
      assertThat((Object) row.getProperty("lit")).isNull();
      assertThat((Object) row.getProperty("atSize")).isNull();
      assertThat((Object) row.getProperty("last")).isEqualTo(List.of(40));
      assertThat((Object) row.getProperty("toMax")).isEqualTo(List.of(20, 30, 40));
    }
  }

  /**
   * The same rule for a write: a negative index selects nothing, so {@code SET l[-1] = v} writes nothing instead of answering an
   * IndexOutOfBoundsException, while an index past the end keeps growing the list up to it, as it always did.
   */
  @ParameterizedTest(name = "SET {0}")
  @MethodSource("sets")
  void setOutsideTheListIsNotAnError(final String assignment, final List<Object> expected) {
    database.getSchema().createDocumentType(TYPE);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set("l", new ArrayList<>(ORIGINAL)).save().getIdentity());

    database.transaction(() -> database.command("sql", "UPDATE " + rid[0] + " SET " + assignment).close());

    assertThat((List<Object>) database.lookupByRID(rid[0], true).asDocument().get("l")).as("SET %s", assignment)
        .containsExactlyElementsOf(expected);
  }

  static Stream<Arguments> sets() {
    return Stream.of( //
        Arguments.of("l[5] = 1", Arrays.asList(10, 20, 30, 40, null, 1)), //
        Arguments.of("l[-1] = 1", ORIGINAL));
  }
}
