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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10654 (item 4): {@link PageId#hashCode()} is computed without the varargs array and the boxed ints of
 * {@code Objects.hash}, and must return exactly the value that {@code Objects.hash(database, fileId, pageNumber)} returned.
 */
class Issue10654PageIdHashCodeTest extends TestHelper {

  private static final int[] VALUES = { 0, 1, 2, 31, 255, 256, 65_535, 65_536, 1_000_003, Integer.MAX_VALUE - 1, Integer.MAX_VALUE,
      -1, -2, -31, -65_536, Integer.MIN_VALUE + 1, Integer.MIN_VALUE };

  @Test
  void hashCodeIsTheValueObjectsHashReturned() {
    for (final int fileId : VALUES)
      for (final int pageNumber : VALUES) {
        final PageId pageId = new PageId(database, fileId, pageNumber);
        assertThat(pageId.hashCode()).as("fileId %d, pageNumber %d", fileId, pageNumber)
            .isEqualTo(Objects.hash(database, fileId, pageNumber));
        // a second call answers from the cached value and agrees
        assertThat(pageId.hashCode()).isEqualTo(Objects.hash(database, fileId, pageNumber));
      }
  }

  @Test
  void equalPageIdsHashAlikeAndKeepTheirKeyIdentityInAMap() {
    final Map<PageId, Integer> map = new HashMap<>();
    for (int fileId = 0; fileId < 20; fileId++)
      for (int pageNumber = 0; pageNumber < 50; pageNumber++)
        map.put(new PageId(database, fileId, pageNumber), fileId * 1000 + pageNumber);

    assertThat(map).hasSize(1000);
    for (int fileId = 0; fileId < 20; fileId++)
      for (int pageNumber = 0; pageNumber < 50; pageNumber++) {
        final PageId probe = new PageId(database, fileId, pageNumber);
        assertThat(probe.hashCode()).isEqualTo(new PageId(database, fileId, pageNumber).hashCode());
        assertThat(map.get(probe)).isEqualTo(fileId * 1000 + pageNumber);
      }
    assertThat(map.get(new PageId(database, 20, 0))).isNull();
  }
}
