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
import com.arcadedb.schema.Type;
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
      for (int i = 0; i < 100; i++)
        database.newDocument("Outer").set("id", i).set("s", "s" + i).save();
      // 40 sub-query rows, the even numbers below 80, held as integers, longs and doubles
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
  void aCorrelatedSubQueryWhoseRowsRepeatForSomeOuterRowsAnswersPerRow() {
    database.getSchema().createDocumentType("OuterR");
    database.getSchema().createDocumentType("InnerR");
    database.transaction(() -> {
      // two outer rows per group, so a cached sub-query result may come back for the second row of a group before the group changes
      for (int id = 0; id < 6; id++)
        database.newDocument("OuterR").set("id", id).set("g", id / 2).save();
      for (int g = 0; g < 3; g++)
        for (int i = 0; i < 40; i++)
          database.newDocument("InnerR").set("g", g).set("n", g * 1000 + i).save();
    });

    // every outer row finds its value among the rows of its own group (rows 2..5 would miss it in the rows of group 0), and a value
    // past the 40 rows of each group matches nothing
    assertThat(ids("SELECT id FROM OuterR WHERE (g * 1000 + id) IN (SELECT n FROM InnerR WHERE g = $parent.$current.g) ORDER BY id"))
        .containsExactly(0, 1, 2, 3, 4, 5);
    assertThat(ids("SELECT id FROM OuterR WHERE (g * 1000 + 40 + id) IN (SELECT n FROM InnerR WHERE g = $parent.$current.g) ORDER BY id"))
        .isEmpty();
    assertThat(ids("SELECT id FROM OuterR WHERE (g * 1000 + id) NOT IN (SELECT n FROM InnerR WHERE g = $parent.$current.g) ORDER BY id"))
        .isEmpty();
  }

  @Test
  void floatAndDoubleRowsAnswerAsTheScanDoes() {
    database.getSchema().createDocumentType("OuterF").createProperty("f", Type.FLOAT);
    database.getSchema().createDocumentType("InnerF").createProperty("d", Type.DOUBLE);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("OuterF").set("id", i).set("f", i / 10f).save();
      // 40 rows, split in two halves of 20, each one short enough to be scanned
      for (int i = 0; i < 80; i += 2)
        database.newDocument("InnerF").set("id", i).set("d", i / 10.0).set("half", i % 4 == 0 ? 0 : 1).save();
    });

    // a FLOAT property probed by DOUBLE rows, against the same rows in two scanned halves
    final List<Integer> probed = ids("SELECT id FROM OuterF WHERE f IN (SELECT d FROM InnerF) ORDER BY id");
    assertThat(probed).hasSize(40)
        .isEqualTo(ids("SELECT id FROM OuterF WHERE f IN (SELECT d FROM InnerF WHERE half = 0) OR f IN (SELECT d FROM InnerF WHERE half = 1) ORDER BY id"));
    assertThat(ids("SELECT id FROM OuterF WHERE f NOT IN (SELECT d FROM InnerF) ORDER BY id")).hasSize(60).isEqualTo(
        ids("SELECT id FROM OuterF WHERE f NOT IN (SELECT d FROM InnerF WHERE half = 0) AND f NOT IN (SELECT d FROM InnerF WHERE half = 1) ORDER BY id"));

    // a DOUBLE property probed by the 100 FLOAT rows, against the same rows in four scanned quarters
    assertThat(ids("SELECT id FROM InnerF WHERE d IN (SELECT f FROM OuterF) ORDER BY id")).hasSize(40).isEqualTo(ids(
        "SELECT id FROM InnerF WHERE d IN (SELECT f FROM OuterF WHERE id < 25) OR d IN (SELECT f FROM OuterF WHERE id >= 25 AND id < 50)"
            + " OR d IN (SELECT f FROM OuterF WHERE id >= 50 AND id < 75) OR d IN (SELECT f FROM OuterF WHERE id >= 75) ORDER BY id"));
  }

  @Test
  void aNullAmongStringRowsLeavesTheMissesOfNotInUnknown() {
    database.getSchema().createDocumentType("OuterN");
    database.getSchema().createDocumentType("InnerN");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("OuterN").set("id", i).set("s", "v" + i).save();
      for (int i = 0; i < 80; i += 2)
        database.newDocument("InnerN").set("t", "v" + i).save();
      database.newDocument("InnerN").set("t", (Object) null).save();
    });

    final List<Integer> even = new ArrayList<>();
    for (int i = 0; i < 80; i += 2)
      even.add(i);
    assertThat(ids("SELECT id FROM OuterN WHERE s IN (SELECT t FROM InnerN) ORDER BY id")).isEqualTo(even);
    // a hit is not in the NOT IN, and a miss is unknown against the null row: nothing answers
    assertThat(ids("SELECT id FROM OuterN WHERE s NOT IN (SELECT t FROM InnerN) ORDER BY id")).isEmpty();
    assertThat(ids("SELECT id FROM OuterN WHERE s NOT IN (SELECT t FROM InnerN WHERE t IS NOT NULL) ORDER BY id")).hasSize(60);
  }

  @Test
  void aLargeSubQueryIsNotScannedOncePerOuterRow() {
    final int outer = 40_000;
    // measured on a laptop: the scan per outer row takes about 11 s, the probe about 0.2 s, the bound sits between them
    final int inner = 20_000;
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
    watch.assertGaveUpWithin(2_000,"one probe of the sub-query rows from a scan of " + inner + " rows per outer row of " + outer);
  }
}
