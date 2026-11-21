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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An {@code IN (SELECT ...)} whose sub-query does not depend on the row is hoisted into a LET, and the rows it returns used to be
 * scanned linearly for every outer row: 1,000 sub-query rows over 100,000 outer rows took 4 s. Once the same list comes back for a
 * second row it is indexed, and every answer stays the one the scan gives.
 */
class InSubQueryProbeTest extends TestHelper {

  private List<Integer> ids(final String sql) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet result = database.query("sql", sql)) {
      while (result.hasNext())
        ids.add(result.next().getProperty("id"));
    }
    return ids;
  }

  @Test
  void aLargeSubQueryAnswersAsTheScanDoes() {
    database.getSchema().createDocumentType("Outer");
    database.getSchema().createDocumentType("Inner");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Outer").set("id", i).set("s", "s" + i).save();
        // 40 sub-query rows: the even numbers below 80, a long, a double, one string-valued row apart
      }
      for (int i = 0; i < 40; i++)
        database.newDocument("Inner").set("n", i % 2 == 0 ? (Object) (i * 2) : i % 3 == 0 ? (Object) (long) (i * 2) : (Object) (double) (i * 2)).save();
    });

    final List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 100; i++)
      if (i < 80 && i % 2 == 0)
        expected.add(i);

    assertThat(ids("SELECT id FROM Outer WHERE id IN (SELECT n FROM Inner) ORDER BY id")).isEqualTo(expected);

    final List<Integer> others = new ArrayList<>();
    for (int i = 0; i < 100; i++)
      if (!expected.contains(i))
        others.add(i);
    assertThat(ids("SELECT id FROM Outer WHERE id NOT IN (SELECT n FROM Inner) ORDER BY id")).isEqualTo(others);

    // a null among the sub-query rows makes a miss unknown: NOT IN answers nothing for the misses, IN is untouched
    database.transaction(() -> database.newDocument("Inner").set("n", (Object) null).save());
    assertThat(ids("SELECT id FROM Outer WHERE id IN (SELECT n FROM Inner) ORDER BY id")).isEqualTo(expected);
    assertThat(ids("SELECT id FROM Outer WHERE id NOT IN (SELECT n FROM Inner) ORDER BY id")).isEmpty();

    // a string column against a numeric left value and the other way round
    assertThat(ids("SELECT id FROM Outer WHERE s IN (SELECT n FROM Inner) ORDER BY id")).isEmpty();
  }

  @Test
  void aLargeSubQueryOfStringsAndALeftValueOfAnotherKindFallBackToTheScan() {
    database.getSchema().createDocumentType("OuterS");
    database.getSchema().createDocumentType("InnerS");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("OuterS").set("id", i).set("s", "v" + i).save();
      for (int i = 0; i < 50; i++)
        database.newDocument("InnerS").set("t", "v" + (i * 2)).save();
      database.newDocument("InnerS").set("t", "7").save();
    });

    assertThat(ids("SELECT id FROM OuterS WHERE s IN (SELECT t FROM InnerS) ORDER BY id")).hasSize(50).first().isEqualTo(0);
    // the number 7 equals the text "7" of the sub-query, as it does for a small one
    assertThat(ids("SELECT id FROM OuterS WHERE id IN (SELECT t FROM InnerS) ORDER BY id")).containsExactly(7);
  }

  @Test
  void aSubQueryThatDependsOnTheOuterRowIsNeverIndexed() {
    database.getSchema().createDocumentType("OuterC");
    database.getSchema().createDocumentType("InnerC");
    database.transaction(() -> {
      for (int g = 0; g < 3; g++)
        database.newDocument("OuterC").set("id", g).save();
      for (int g = 0; g < 3; g++)
        for (int i = 0; i < 40; i++)
          database.newDocument("InnerC").set("g", g).set("n", g * 1000 + i).save();
    });

    // outer row g sees only the sub-query rows of group g, 40 of them each
    final List<Integer> ids = ids("SELECT id FROM OuterC WHERE (id * 1000 + 5) IN (SELECT n FROM InnerC WHERE g = $parent.$current.id) ORDER BY id");
    assertThat(ids).containsExactly(0, 1, 2);
    assertThat(ids("SELECT id FROM OuterC WHERE (id * 1000 + 45) IN (SELECT n FROM InnerC WHERE g = $parent.$current.id) ORDER BY id")).isEmpty();
  }

  @Test
  void aLargeSubQueryIsNotScannedOncePerOuterRow() {
    final int outer = 40_000;
    final int inner = 8_000;
    database.getSchema().createDocumentType("OuterP");
    database.getSchema().createDocumentType("InnerP");
    database.transaction(() -> {
      for (int i = 0; i < outer; i++)
        database.newDocument("OuterP").set("id", i).save();
      for (int i = 0; i < inner; i++)
        database.newDocument("InnerP").set("n", i * 10).save();
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    long matched = 0;
    try (final ResultSet result = database.query("sql", "SELECT id FROM OuterP WHERE id IN (SELECT n FROM InnerP)")) {
      while (result.hasNext()) {
        result.next();
        matched++;
      }
    }

    assertThat(matched).isEqualTo(outer / 10);
    watch.assertGaveUpWithin(5_000, "one probe of the sub-query rows from a scan of " + inner + " rows per outer row of " + outer);
  }
}
