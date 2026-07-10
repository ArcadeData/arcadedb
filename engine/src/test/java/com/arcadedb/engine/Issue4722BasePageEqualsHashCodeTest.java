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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4722.
 * <p>
 * {@link BasePage#equals(Object)} used to key on {@code (pageId, version)} while {@link BasePage#hashCode()}
 * keyed on {@code pageId} only. Because {@code version} is mutable ({@link MutablePage#incrementVersion()}),
 * two pages for the same {@link PageId} could flip between equal and not-equal over their lifetime while their
 * hash code never changed. Tying value equality to a mutable field is the same fragility that caused #4544.
 * <p>
 * Both methods now key on {@code pageId} only, so equality is stable across version changes and fully
 * consistent with the hash code.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4722BasePageEqualsHashCodeTest extends TestHelper {

  /**
   * Two pages for the same {@link PageId} must be equal and share the same hash code regardless of their
   * (mutable) version, and equality must not change when the version is incremented.
   */
  @Test
  void equalsIsStableAcrossVersionChangesAndConsistentWithHashCode() {
    final PageId pageId = new PageId(database, 3, 7);

    final MutablePage a = new MutablePage(pageId, 1024, new byte[1024], 5, 0);
    final MutablePage b = new MutablePage(pageId, 1024, new byte[1024], 9, 0);

    // Same pageId, different version: equal and same hash code (version is not part of identity).
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());

    // Mutating the version must not change equality nor the hash code (it would, when version was part of equals).
    b.incrementVersion();
    assertThat(a).as("equality must stay stable across version changes").isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());

    a.incrementVersion();
    a.incrementVersion();
    assertThat(a).as("equality is independent of how many times version changed").isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  /**
   * Pages for different {@link PageId}s are never equal.
   */
  @Test
  void differentPageIdsAreNotEqual() {
    final MutablePage a = new MutablePage(new PageId(database, 3, 7), 1024, new byte[1024], 1, 0);
    final MutablePage differentPage = new MutablePage(new PageId(database, 3, 8), 1024, new byte[1024], 1, 0);
    final MutablePage differentFile = new MutablePage(new PageId(database, 4, 7), 1024, new byte[1024], 1, 0);

    assertThat(a).isNotEqualTo(differentPage);
    assertThat(a).isNotEqualTo(differentFile);
  }

  /**
   * A {@link MutablePage} and an {@link ImmutablePage} with the same {@link PageId} are different types and must
   * not be equal (class-sensitive equality is preserved).
   */
  @Test
  void differentPageTypesAreNotEqual() {
    final PageId pageId = new PageId(database, 3, 7);
    final MutablePage mutable = new MutablePage(pageId, 1024, new byte[1024], 1, 0);
    final ImmutablePage immutable = new ImmutablePage(pageId, 1024, new byte[1024], 1, 0);

    assertThat(mutable).isNotEqualTo(immutable);
  }

  /**
   * The general {@code equals}/{@code hashCode} contract: equal pages must share the same hash code.
   */
  @Test
  void equalPagesShareHashCode() {
    final PageId pageId = new PageId(database, 1, 2);
    final MutablePage a = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
    final MutablePage b = new MutablePage(pageId, 1024, new byte[1024], 0, 0);

    if (a.equals(b))
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }
}
