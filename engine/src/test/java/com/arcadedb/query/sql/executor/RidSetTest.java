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

import static org.junit.jupiter.api.Assertions.*;

public class RidSetTest extends TestHelper {

  @Test
  public void testAddAndContains() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    // Test add() returns true when adding new element
    assertTrue(set.add(rid1));
    assertTrue(set.add(rid2));
    assertTrue(set.add(rid3));

    // Test add() returns false when adding duplicate
    assertFalse(set.add(rid1));
    assertFalse(set.add(rid2));

    // Test contains()
    assertTrue(set.contains(rid1));
    assertTrue(set.contains(rid2));
    assertTrue(set.contains(rid3));
    assertFalse(set.contains(new RID(database, 3, 300)));

    // Test size()
    assertEquals(3, set.size());
  }

  @Test
  public void testRemove() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);

    set.add(rid1);
    set.add(rid2);

    // Test remove() returns true when element exists
    assertTrue(set.remove(rid1));
    assertFalse(set.contains(rid1));
    assertEquals(1, set.size());

    // Test remove() returns false when element doesn't exist
    assertFalse(set.remove(rid1));
    assertEquals(1, set.size());

    // Remove remaining element
    assertTrue(set.remove(rid2));
    assertTrue(set.isEmpty());
  }

  @Test
  public void testAddAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    final List<RID> rids = Arrays.asList(rid1, rid2, rid3);

    // Test addAll() returns true when any element is added
    assertTrue(set.addAll(rids));
    assertEquals(3, set.size());

    // Test addAll() returns false when all elements already exist
    assertFalse(set.addAll(rids));
    assertEquals(3, set.size());

    // Test addAll() returns true when at least one element is new
    final List<RID> moreRids = Arrays.asList(rid2, new RID(database, 3, 100));
    assertTrue(set.addAll(moreRids));
    assertEquals(4, set.size());
  }

  @Test
  public void testIteratorWithContext() {
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

    assertEquals(3, iteratedRids.size());
    assertTrue(iteratedRids.contains(rid1));
    assertTrue(iteratedRids.contains(rid2));
    assertTrue(iteratedRids.contains(rid3));
  }

  @Test
  public void testClear() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);

    set.add(rid1);
    set.add(rid2);

    assertFalse(set.isEmpty());
    assertEquals(2, set.size());

    set.clear();

    assertTrue(set.isEmpty());
    assertEquals(0, set.size());
    assertFalse(set.contains(rid1));
    assertFalse(set.contains(rid2));
  }

  @Test
  public void testContainsAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    set.add(rid1);
    set.add(rid2);

    assertTrue(set.containsAll(Arrays.asList(rid1, rid2)));
    assertFalse(set.containsAll(Arrays.asList(rid1, rid2, rid3)));
  }

  @Test
  public void testRemoveAll() {
    final RidSet set = new RidSet();
    final RID rid1 = new RID(database, 1, 100);
    final RID rid2 = new RID(database, 1, 200);
    final RID rid3 = new RID(database, 2, 100);

    set.add(rid1);
    set.add(rid2);
    set.add(rid3);

    set.removeAll(Arrays.asList(rid1, rid3));

    assertEquals(1, set.size());
    assertFalse(set.contains(rid1));
    assertTrue(set.contains(rid2));
    assertFalse(set.contains(rid3));
  }
}
