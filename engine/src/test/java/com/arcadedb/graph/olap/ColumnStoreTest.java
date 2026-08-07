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
package com.arcadedb.graph.olap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@link ColumnStore}'s single-slot column-resolution cache
 * (issue #5745): {@code getColumn()}/{@code getValue()} are called with the same
 * property name across many rows in query loops, so the last-resolved column is
 * memoized to avoid a repeated {@code HashMap} probe on the property name. These
 * tests exercise the cache under access patterns that would expose a torn or
 * stale entry: alternating between properties, repeating the same property,
 * probing a missing property, and probing the empty store.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ColumnStoreTest {

  @Test
  void repeatedAccessToSamePropertyReturnsCorrectValues() {
    final ColumnStore store = new ColumnStore(3);
    final Column city = store.createColumn("city", Column.Type.STRING);
    city.setString(0, "Rome");
    city.setString(1, "Milan");
    city.setString(2, "Turin");

    // Same property name probed repeatedly (the hot-loop pattern the cache targets).
    for (int i = 0; i < 5; i++) {
      assertThat(store.getValue(0, "city")).isEqualTo("Rome");
      assertThat(store.getValue(1, "city")).isEqualTo("Milan");
      assertThat(store.getValue(2, "city")).isEqualTo("Turin");
    }
  }

  @Test
  void alternatingPropertiesResolveIndependently() {
    final ColumnStore store = new ColumnStore(2);
    final Column name = store.createColumn("name", Column.Type.STRING);
    name.setString(0, "Alice");
    name.setString(1, "Bob");
    final Column age = store.createColumn("age", Column.Type.INT);
    age.setInt(0, 30);
    age.setInt(1, 40);

    // Alternate property names on every call so the cache slot is invalidated and
    // re-resolved each time; must never leak the wrong column's value across the switch.
    for (int i = 0; i < 10; i++) {
      assertThat(store.getValue(0, "name")).isEqualTo("Alice");
      assertThat(store.getValue(0, "age")).isEqualTo(30);
      assertThat(store.getValue(1, "name")).isEqualTo("Bob");
      assertThat(store.getValue(1, "age")).isEqualTo(40);
    }
  }

  @Test
  void missingPropertyDoesNotPoisonSubsequentLookups() {
    final ColumnStore store = new ColumnStore(2);
    final Column name = store.createColumn("name", Column.Type.STRING);
    name.setString(0, "Alice");

    // Probe an absent property first (caches a null column), then a real one with a
    // DIFFERENT name, then the absent one again — none of these should observe the
    // other's cached entry.
    assertThat(store.getValue(0, "missing")).isNull();
    assertThat(store.getValue(0, "name")).isEqualTo("Alice");
    assertThat(store.getValue(0, "missing")).isNull();
    assertThat(store.getColumn("missing")).isNull();
    assertThat(store.getColumn("name")).isSameAs(name);
  }

  @Test
  void repeatedMissingPropertyLookupStaysNull() {
    final ColumnStore store = new ColumnStore(1);
    store.createColumn("name", Column.Type.STRING);

    for (int i = 0; i < 5; i++)
      assertThat(store.getValue(0, "doesNotExist")).isNull();
  }

  @Test
  void nullValueWithinExistingColumnIsDistinctFromMissingColumn() {
    final ColumnStore store = new ColumnStore(2);
    final Column age = store.createColumn("age", Column.Type.INT);
    age.setInt(0, 25); // node 1 left unset -> null bit stays set

    assertThat(store.getValue(0, "age")).isEqualTo(25);
    assertThat(store.getValue(1, "age")).isNull();
    // Re-probe node 0 right after a null read on the SAME column/property to confirm
    // the cache (keyed on name, not on null-ness) still resolves the right column.
    assertThat(store.getValue(0, "age")).isEqualTo(25);
  }

  @Test
  void getColumnOnEmptyStoreReturnsNull() {
    final ColumnStore store = new ColumnStore(0);
    assertThat(store.getColumn("anything")).isNull();
    assertThat(store.getValue(0, "anything")).isNull();
  }
}
