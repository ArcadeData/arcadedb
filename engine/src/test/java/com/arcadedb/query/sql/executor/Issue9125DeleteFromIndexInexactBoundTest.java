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
import com.arcadedb.index.Index;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9125: {@code DELETE FROM INDEX:<name> WHERE key ...} on an integral key (BYTE, SHORT, INTEGER, LONG) passed a
 * fractional or out-of-range bound to the index, which truncates or clamps it, so {@code key BETWEEN 11.5 AND 12.5} also
 * removed the entries of key 11. The delete must remove exactly the keys a scan with the same condition matches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9125DeleteFromIndexInexactBoundTest extends TestHelper {

  private static final String[] KEY_TYPES = { "BYTE", "SHORT", "INTEGER", "LONG" };

  /** Keys 9..14 in a fresh indexed type, deletes through the index and returns the keys left, ascending. */
  private List<Integer> deleteAndGetRemaining(final String keyType, final String condition, final Object... params) {
    final String type = "T" + keyType + Math.abs(condition.hashCode());
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".k " + keyType);
      database.command("sql", "CREATE INDEX ON " + type + " (k) NOTUNIQUE");
      for (int k = 9; k <= 14; k++)
        database.command("sql", "INSERT INTO " + type + " SET k = " + k);
    });
    final Index index = database.getSchema().getType(type).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    database.transaction(() -> {
      // the entries are removed as the result set is pulled
      try (final ResultSet resultSet = database.command("sql", "DELETE FROM INDEX:`" + index.getName() + "` WHERE " + condition, params)) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    });

    final List<Integer> remaining = new ArrayList<>();
    // the entries of a record whose index entry is gone are not found through the index, so ask the index itself
    final var cursor = index.get(new Object[] { 0 });
    cursor.close();
    for (int k = 9; k <= 14; k++) {
      final var c = index.get(new Object[] { k });
      if (c.hasNext())
        remaining.add(k);
      c.close();
    }
    return remaining;
  }

  @Test
  void aNonNumericBoundNeverSurfacesAsAClassCastException() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TNonNum9125");
      database.command("sql", "CREATE PROPERTY TNonNum9125.k INTEGER");
      database.command("sql", "CREATE INDEX ON TNonNum9125 (k) NOTUNIQUE");
      database.command("sql", "INSERT INTO TNonNum9125 SET k = 1");
    });
    final Index index = database.getSchema().getType("TNonNum9125").getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
    try {
      database.transaction(() -> {
        try (final ResultSet resultSet = database.command("sql", "DELETE FROM INDEX:`" + index.getName() + "` WHERE key BETWEEN 11.5 AND 'x'")) {
          while (resultSet.hasNext())
            resultSet.next();
        }
      });
    } catch (final RuntimeException e) {
      assertThat(e).isNotInstanceOf(ClassCastException.class);
    }
  }

  @Test
  void equalityWithAnExactBoundRemovesTheKey() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key = 12")).as(keyType).containsExactly(9, 10, 11, 13, 14);
  }

  @Test
  void exactBoundsRemoveExactlyTheKeysTheOperatorSelects() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key > 12")).as(keyType + " >").containsExactly(9, 10, 11, 12);
      assertThat(deleteAndGetRemaining(keyType, "key >= 12")).as(keyType + " >=").containsExactly(9, 10, 11);
      assertThat(deleteAndGetRemaining(keyType, "key < 12")).as(keyType + " <").containsExactly(12, 13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key <= 12")).as(keyType + " <=").containsExactly(13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 10 AND 12")).as(keyType + " BETWEEN").containsExactly(9, 13, 14);
    }
  }

  @Test
  void lowerThanWithAFractionalBoundRemovesOnlyTheKeysBelow() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key < 11.5")).as(keyType + " <").containsExactly(12, 13, 14);
      assertThat(deleteAndGetRemaining(keyType, "key <= 11.5")).as(keyType + " <=").containsExactly(12, 13, 14);
    }
  }

  @Test
  void betweenWithFractionalBoundsRemovesOnlyTheKeysInside() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 11.5 AND 12.5")).as(keyType).containsExactly(9, 10, 11, 13, 14);
  }

  @Test
  void betweenWithAFractionalBoundParameterRemovesOnlyTheKeysInside() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN ? AND ?", 10.5d, 13.5d)).as(keyType).containsExactly(9, 10, 14);
  }

  @Test
  void betweenWithNoKeyInsideRemovesNothing() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN 11.2 AND 11.8")).as(keyType).containsExactly(9, 10, 11, 12, 13, 14);
  }

  @Test
  void betweenWithBoundsPastTheKeyRangeRemovesEveryKey() {
    for (final String keyType : new String[] { "LONG" })
      assertThat(deleteAndGetRemaining(keyType, "key BETWEEN -1e30 AND 1e30")).as(keyType).isEmpty();
  }

  @Test
  void equalityWithAFractionalBoundRemovesNothing() {
    for (final String keyType : KEY_TYPES)
      assertThat(deleteAndGetRemaining(keyType, "key = 12.5")).as(keyType).containsExactly(9, 10, 11, 12, 13, 14);
  }

  @Test
  void greaterThanWithAFractionalBoundRemovesOnlyTheKeysAbove() {
    for (final String keyType : KEY_TYPES) {
      assertThat(deleteAndGetRemaining(keyType, "key > 11.5")).as(keyType + " >").containsExactly(9, 10, 11);
      assertThat(deleteAndGetRemaining(keyType, "key >= 12.5")).as(keyType + " >=").containsExactly(9, 10, 11, 12);
    }
  }

  @Test
  void fractionalBoundsAtTheEdgesOfTheKeyRangeAreHandled() {
    // Integer.MAX_VALUE + 0.5 is above every INTEGER key, -Integer.MAX_VALUE - 1.5 below every one
    assertThat(deleteAndGetRemaining("INTEGER", "key > 2147483647.5")).as("> above the range").containsExactly(9, 10, 11, 12, 13, 14);
    assertThat(deleteAndGetRemaining("INTEGER", "key < -2147483648.5")).as("< below the range").containsExactly(9, 10, 11, 12, 13, 14);
    assertThat(deleteAndGetRemaining("INTEGER", "key >= -2147483648.5")).as(">= below the range").isEmpty();
    assertThat(deleteAndGetRemaining("INTEGER", "key <= 2147483647.5")).as("<= above the range").isEmpty();
  }
}
