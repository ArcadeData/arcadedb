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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class RidSetTest extends TestHelper {

  @Test
  void addAndContains() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    // Test add() returns true when adding new element
    assertThat(set.add(rid1)).isTrue();
    assertThat(set.add(rid2)).isTrue();
    assertThat(set.add(rid3)).isTrue();

    // Test add() returns false when adding duplicate
    assertThat(set.add(rid1)).isFalse();
    assertThat(set.add(rid2)).isFalse();

    // Test contains()
    assertThat(set.contains(rid1)).isTrue();
    assertThat(set.contains(rid2)).isTrue();
    assertThat(set.contains(rid3)).isTrue();
    assertThat(set.contains(new RID(database, 3, 300))).isFalse();

    // Test size()
    assertThat(set.size()).isEqualTo(3);
  }

  @Test
  void remove() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);

    set.add(rid1);
    set.add(rid2);

    // Test remove() returns true when element exists
    assertThat(set.remove(rid1)).isTrue();
    assertThat(set.contains(rid1)).isFalse();
    assertThat(set.size()).isEqualTo(1);

    // Test remove() returns false when element doesn't exist
    assertThat(set.remove(rid1)).isFalse();
    assertThat(set.size()).isEqualTo(1);

    // Remove remaining element
    assertThat(set.remove(rid2)).isTrue();
    assertThat(set.isEmpty()).isTrue();
  }

  @Test
  void addAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    final List<RID> rids = Arrays.asList(rid1, rid2, rid3);

    // Test addAll() returns true when any element is added
    assertThat(set.addAll(rids)).isTrue();
    assertThat(set.size()).isEqualTo(3);

    // Test addAll() returns false when all elements already exist
    assertThat(set.addAll(rids)).isFalse();
    assertThat(set.size()).isEqualTo(3);

    // Test addAll() returns true when at least one element is new
    final List<RID> moreRids = Arrays.asList(rid2, new RID(database, 3, 100));
    assertThat(set.addAll(moreRids)).isTrue();
    assertThat(set.size()).isEqualTo(4);
  }

  @Test
  void iteratorWithContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final RidSet set = new RidSet(context);
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    set.add(rid1);
    set.add(rid2);
    set.add(rid3);

    // Test iterator works with context
    final Set<RID> iteratedRids = new HashSet<>();
    for (final RID rid : set) {
      iteratedRids.add(rid);
    }

    assertThat(iteratedRids.size()).isEqualTo(3);
    assertThat(iteratedRids.contains(rid1)).isTrue();
    assertThat(iteratedRids.contains(rid2)).isTrue();
    assertThat(iteratedRids.contains(rid3)).isTrue();
  }

  @Test
  void clear() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);

    set.add(rid1);
    set.add(rid2);

    assertThat(set.isEmpty()).isFalse();
    assertThat(set.size()).isEqualTo(2);

    set.clear();

    assertThat(set.isEmpty()).isTrue();
    assertThat(set.size()).isEqualTo(0);
    assertThat(set.contains(rid1)).isFalse();
    assertThat(set.contains(rid2)).isFalse();
  }

  @Test
  void containsAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    set.add(rid1);
    set.add(rid2);

    assertThat(set.containsAll(Arrays.asList(rid1, rid2))).isTrue();
    assertThat(set.containsAll(Arrays.asList(rid1, rid2, rid3))).isFalse();
  }

  @Test
  void removeAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    set.add(rid1);
    set.add(rid2);
    set.add(rid3);

    set.removeAll(Arrays.asList(rid1, rid3));

    assertThat(set.size()).isEqualTo(1);
    assertThat(set.contains(rid1)).isFalse();
    assertThat(set.contains(rid2)).isTrue();
    assertThat(set.contains(rid3)).isFalse();
  }
}
