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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8049
 * <p>
 * {@code SelectOperator.lt}/{@code le} compared through {@code BinaryComparator.compareTo()}, which is a SORT order
 * and answers {@code -1} for a null left operand. Read as a predicate that made a record WITHOUT the property - or
 * carrying it as an explicit null - satisfy every {@code <} and {@code <=} on it, while {@code gt}/{@code ge} were
 * unaffected by the same {@code -1}: the two directions disagreed.
 * <p>
 * Worse, the answer was not even stable. When an index exists on the property, {@code filterWithIndexesFinalNode()}
 * answers the leaf from a range scan, and an index holds no entry for a record that lacks the property - so the
 * null-bearing records were never offered to {@code evaluateWhere()} at all. The same select over the same data
 * returned 12 rows with no index and 5 with one, which is the one thing adding an index must never do.
 * <p>
 * Each predicate below is therefore asserted three ways: against SQL, without an index, and with one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8049SelectNullOrderingComparisonTest extends TestHelper {

  private static final int WITH_VALUE  = 5;
  private static final int WITHOUT_A   = 5;
  private static final int EXPLICIT_NULL = 2;

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("tag", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < WITH_VALUE; i++) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("a", i);
        doc.set("tag", "valued");
        doc.save();
      }
      for (int i = 0; i < WITHOUT_A; i++) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("tag", "absent");
        doc.save();
      }
      for (int i = 0; i < EXPLICIT_NULL; i++) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("a", null);
        doc.set("tag", "null");
        doc.save();
      }
    });
  }

  @Test
  void lessThanDoesNotMatchRecordsWithoutTheProperty() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").lt().value(5).count(),
        "a < 5", WITH_VALUE);
  }

  @Test
  void lessThanOrEqualDoesNotMatchRecordsWithoutTheProperty() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").le().value(4).count(),
        "a <= 4", WITH_VALUE);
  }

  @Test
  void greaterThanStaysConsistent() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").gt().value(-1).count(),
        "a > -1", WITH_VALUE);
  }

  @Test
  void greaterThanOrEqualStaysConsistent() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("a").ge().value(0).count(),
        "a >= 0", WITH_VALUE);
  }

  @Test
  void betweenStaysConsistent() {
    assertSameWithAndWithoutIndex(
        () -> database.select().fromType("D").where().property("a").between().values(0, 4).count(), "a between 0 and 4",
        WITH_VALUE);
  }

  @Test
  void isNullStillSeesEveryRecordWithoutAValue() {
    // THE COMPLEMENTARY QUESTION IS THE ONE THAT ANSWERS "WHICH RECORDS HAVE NO VALUE" - IT MUST KEEP ANSWERING IT
    assertThat(database.select().fromType("D").where().property("a").isNull().count()).isEqualTo(
        WITHOUT_A + EXPLICIT_NULL);
    assertThat(database.select().fromType("D").where().property("a").isNotNull().count()).isEqualTo(WITH_VALUE);
  }

  @Test
  void lessThanAndGreaterThanOrEqualAreNoLongerJointlyExhaustive() {
    // THE OLD BEHAVIOUR MADE `a < 5` AND `a >= 5` COVER EVERY RECORD WHILE `a is null` WAS ALSO TRUE FOR SOME OF
    // THEM. THE TWO HALVES NOW SUM TO THE RECORDS THAT ACTUALLY CARRY A VALUE, AND NO MORE
    final long below = database.select().fromType("D").where().property("a").lt().value(5).count();
    final long atOrAbove = database.select().fromType("D").where().property("a").ge().value(5).count();
    assertThat(below + atOrAbove).isEqualTo(WITH_VALUE);
  }

  @Test
  void aNullRightOperandNeverMatches() {
    // SQL'S OWN RULE: A COMPARISON WITH NULL IS UNKNOWN, AND UNKNOWN DOES NOT MATCH. compareTo(x, null) ANSWERS 1,
    // SO `a > null` USED TO BE TRUE FOR EVERY RECORD CARRYING AN 'a'
    assertThat(database.select().fromType("D").where().property("a").gt().value(null).count()).isZero();
    assertThat(database.select().fromType("D").where().property("a").ge().value(null).count()).isZero();
    assertThat(database.select().fromType("D").where().property("a").lt().value(null).count()).isZero();
    assertThat(database.select().fromType("D").where().property("a").le().value(null).count()).isZero();
  }

  private void assertSameWithAndWithoutIndex(final java.util.function.LongSupplier nativeCount, final String sqlWhere,
      final long expected) {
    assertThat(sqlCount(sqlWhere)).as("SQL baseline for " + sqlWhere).isEqualTo(expected);
    assertThat(nativeCount.getAsLong()).as("native without an index: " + sqlWhere).isEqualTo(expected);

    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a");
    try {
      assertThat(nativeCount.getAsLong()).as("native with an index: " + sqlWhere).isEqualTo(expected);
    } finally {
      for (final var index : database.getSchema().getType("D").getIndexesByProperties("a"))
        database.getSchema().dropIndex(index.getName());
    }
  }

  private long sqlCount(final String where) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + where).nextIfAvailable().getProperty("c"))
        .longValue();
  }
}
