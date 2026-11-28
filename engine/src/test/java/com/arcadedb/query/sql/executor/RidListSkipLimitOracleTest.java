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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle test of {@code SKIP} and {@code LIMIT} over a {@code FROM [#rid, ...]} target, the statement shape an application sends
 * to rank or page a candidate list. The list is longer than one pull batch, holds duplicates and a RID that never existed, and
 * is not in storage order, so a skip or a limit that lands on a batch edge, inside the duplicates or past the end is exercised.
 * The model is the list itself: a record once per occurrence, in list order, without the RID that does not resolve.
 */
class RidListSkipLimitOracleTest extends TestHelper {
  private static final int   DOCUMENTS = 300;
  private static final int[] SKIPS     = { 0, 1, 2, 99, 100, 101, 199, 200, 299, 319, 320, 500 };
  private static final int[] LIMITS    = { 0, 1, 2, 99, 100, 101, 250, 1000 };

  /** The list as sent, with the never existing RID in it. */
  private final List<String>  target = new ArrayList<>();
  /** What the list answers: the {@code n} of each occurrence that resolves, in list order. */
  private final List<Integer> answer = new ArrayList<>();
  private final List<Integer> group  = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Row");
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < DOCUMENTS; i++) {
        final MutableDocument document = database.newDocument("Row").set("n", i, "g", i % 3);
        document.save();
        rids.add(document.getIdentity());
      }
    });
    final List<Integer> order = new ArrayList<>();
    for (int i = 0; i < DOCUMENTS; i++)
      order.add(i);
    Collections.shuffle(order, new Random(7));
    for (int i = 0; i < 20; i++)
      order.add(order.get(i));
    for (int position = 0; position < order.size(); position++) {
      if (position == 150)
        target.add("#999:1");
      target.add(rids.get(order.get(position)).toString());
      answer.add(order.get(position));
      group.add(order.get(position) % 3);
    }
  }

  private List<Integer> run(final String where, final String order, final String tail) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT n FROM [" + String.join(", ", target) + "]" + where + order + tail)) {
      rs.forEachRemaining(row -> out.add(row.getProperty("n")));
    }
    return out;
  }

  private static List<Integer> slice(final List<Integer> rows, final int skip, final int limit) {
    final int from = Math.min(skip, rows.size());
    final int to = limit < 0 ? rows.size() : (int) Math.min((long) rows.size(), (long) from + limit);
    return rows.subList(from, to);
  }

  @Test
  void skipAndLimitSliceTheListInListOrder() {
    assertThat(run("", "", "")).containsExactlyElementsOf(answer);
    for (final int skip : SKIPS)
      for (final int limit : LIMITS)
        assertThat(run("", "", " SKIP " + skip + " LIMIT " + limit)).as("SKIP %d LIMIT %d", skip, limit)
            .containsExactlyElementsOf(slice(answer, skip, limit));
    for (final int skip : SKIPS)
      assertThat(run("", "", " SKIP " + skip)).as("SKIP %d", skip).containsExactlyElementsOf(slice(answer, skip, -1));
    for (final int limit : LIMITS)
      assertThat(run("", "", " LIMIT " + limit)).as("LIMIT %d", limit).containsExactlyElementsOf(slice(answer, 0, limit));
  }

  @Test
  void skipAndLimitCountTheRowsThatSurviveTheWhereClause() {
    final List<Integer> kept = new ArrayList<>();
    for (int i = 0; i < answer.size(); i++)
      if (group.get(i) == 1)
        kept.add(answer.get(i));
    assertThat(kept.size()).isGreaterThan(100);
    for (final int skip : SKIPS)
      for (final int limit : LIMITS)
        assertThat(run(" WHERE g = 1", "", " SKIP " + skip + " LIMIT " + limit)).as("SKIP %d LIMIT %d", skip, limit)
            .containsExactlyElementsOf(slice(kept, skip, limit));
  }

  @Test
  void skipAndLimitSliceTheOrderedList() {
    final List<Integer> ordered = new ArrayList<>(answer);
    ordered.sort(Collections.reverseOrder());
    for (final int skip : SKIPS)
      for (final int limit : LIMITS)
        assertThat(run("", " ORDER BY n DESC", " SKIP " + skip + " LIMIT " + limit)).as("SKIP %d LIMIT %d", skip, limit)
            .containsExactlyElementsOf(slice(ordered, skip, limit));
  }

  @Test
  void aSkipAndALimitGivenAsParametersSliceTheListToo() {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT n FROM [" + String.join(", ", target) + "] SKIP :s LIMIT :l",
        Map.of("s", 101, "l", 99))) {
      rs.forEachRemaining(row -> out.add(row.getProperty("n")));
    }
    assertThat(out).containsExactlyElementsOf(slice(answer, 101, 99));
  }
}
