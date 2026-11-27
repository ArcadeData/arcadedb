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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12794: {@code UPDATE ... REMOVE list = <the same list>} iterated the collection it was removing from, so
 * a 2-element list lost only its first element and a longer one failed with a ConcurrentModificationException. It now gives the
 * same answer as a copy of the list on the right side.
 */
class Issue12794RemoveListFromItselfTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE D"));
  }

  @Test
  void removeSelfTwoElements() {
    assertThat(removeAndRead("['a', 'b']", "tags")).isEmpty();
  }

  @Test
  void removeSelfThreeElements() {
    assertThat(removeAndRead("['a', 'b', 'c']", "tags")).isEmpty();
  }

  @Test
  void removeSelfFiveElements() {
    assertThat(removeAndRead("['a', 'b', 'c', 'd', 'e']", "tags")).isEmpty();
  }

  @Test
  void removeCurrentTags() {
    assertThat(removeAndRead("['a', 'b', 'c']", "$current.tags")).isEmpty();
  }

  @Test
  void removeAsList() {
    assertThat(removeAndRead("['a', 'b', 'c']", "tags.asList()")).isEmpty();
  }

  @Test
  void duplicatesMatchACopy() {
    // a copy of the list removes one occurrence per element on the right side, so every duplicate goes too
    final List<Object> copy = removeAndRead("['a', 'a', 'b']", "[tags[0], tags[1], tags[2]]");
    final List<Object> self = removeAndRead("['a', 'a', 'b']", "tags");
    assertThat(self).isEqualTo(copy);
    assertThat(self).isEmpty();
  }

  @Test
  void multiValueRemoveSameObject() {
    final List<Object> list = new ArrayList<>(List.of("a", "b", "c", "a"));
    MultiValue.remove(list, list, false);
    assertThat(list).isEmpty();
  }

  private List<Object> removeAndRead(final String tags, final String right) {
    final AtomicReference<List<Object>> result = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "DELETE FROM D");
      database.command("sql", "INSERT INTO D SET id = 0, tags = " + tags);
      try (final ResultSet rs = database.command("sql", "UPDATE D REMOVE tags = " + right + " WHERE id = 0")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT tags FROM D WHERE id = 0")) {
        result.set(rs.next().getProperty("tags"));
      }
    });
    return result.get();
  }
}
