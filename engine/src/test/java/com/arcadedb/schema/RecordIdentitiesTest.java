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
package com.arcadedb.schema;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class RecordIdentitiesTest {

  @Test
  void sortAndDeduplicateMatchesASortedSet() {
    final Random random = new Random(11475);
    for (final int size : new int[] { 0, 1, 2, 17, 1000, 20_000 }) {
      final RecordIdentities identities = new RecordIdentities(4);
      final TreeSet<RID> expected = new TreeSet<>();
      for (int i = 0; i < size; i++) {
        // narrow ranges so duplicates and equal bucket ids are common
        final RID rid = new RID(random.nextInt(5), random.nextInt(size / 2 + 1));
        identities.add(rid);
        expected.add(rid);
      }

      identities.sortAndDeduplicate();

      final List<RID> actual = new ArrayList<>(identities.size());
      for (int i = 0; i < identities.size(); i++)
        actual.add(identities.get(i));
      assertThat(actual).as("size %d", size).containsExactlyElementsOf(expected);
      for (final RID rid : expected)
        assertThat(identities.contains(rid)).isTrue();
      assertThat(identities.contains(new RID(7, 0))).isFalse();
      assertThat(identities.contains(new RID(0, -1))).isFalse();
    }
  }

  @Test
  void addAllAppendsAndClearEmpties() {
    final RecordIdentities a = new RecordIdentities(1);
    a.add(new RID(1, 1));
    final RecordIdentities b = new RecordIdentities(1);
    for (int i = 0; i < 10; i++)
      b.add(new RID(2, i));

    a.addAll(b);
    assertThat(a.size()).isEqualTo(11);
    assertThat(a.get(0)).isEqualTo(new RID(1, 1));
    assertThat(a.get(10)).isEqualTo(new RID(2, 9));

    a.clear();
    assertThat(a.size()).isZero();
    a.add(new RID(3, 3));
    assertThat(a.get(0)).isEqualTo(new RID(3, 3));
  }
}
