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
package com.arcadedb.function.coll;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CollRemoveTest {

  private final CollRemove fn = new CollRemove();

  @Test
  void removesSingleElementByIndex() {
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { new ArrayList<>(List.of("a", "b", "c")), 1 }, null);
    assertThat(result).containsExactly("a", "c");
  }

  @Test
  void removesCountElementsFromIndex() {
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { new ArrayList<>(List.of("a", "b", "c", "d")), 1, 2 }, null);
    assertThat(result).containsExactly("a", "d");
  }

  @Test
  void nullListReturnsNull() {
    assertThat(fn.execute(new Object[] { null, 0 }, null)).isNull();
  }

  @Test
  void nullIndexReturnsNull() {
    assertThat(fn.execute(new Object[] { new ArrayList<>(List.of("a", "b")), null }, null)).isNull();
  }

  /**
   * A null count argument must behave like the other null arguments (return null) instead of throwing a
   * NullPointerException, keeping the function's null-handling consistent.
   */
  @Test
  void nullCountReturnsNull() {
    assertThat(fn.execute(new Object[] { new ArrayList<>(List.of("a", "b", "c")), 0, null }, null)).isNull();
  }

  @Test
  void doesNotMutateSource() {
    final List<Object> source = new ArrayList<>(List.of("a", "b", "c"));
    fn.execute(new Object[] { source, 0 }, null);
    assertThat(source).containsExactly("a", "b", "c");
  }
}
