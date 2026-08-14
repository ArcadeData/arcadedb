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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct-invocation tests for coll.toSet. The Cypher-level behavior is covered by CypherMissingFunctionsTest;
 * what needs a direct call is the argument shape a Cypher list literal can never produce - a Collection that is
 * not a List, which AbstractCollFunction.asList copies rather than casts.
 */
class CollToSetTest {

  private final CollToSet fn = new CollToSet();

  @Test
  void dedupesACollectionThatIsNotAList() {
    // ArrayDeque is a Collection but not a List, and unlike a Set it can still carry the duplicates under test.
    final Collection<Object> source = new ArrayDeque<>(List.of("a", "b", "a", "c"));
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[] { source }, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void doesNotMutateSource() {
    final List<Object> source = new ArrayList<>(List.of("a", "b", "a"));
    fn.execute(new Object[] { source }, null);
    assertThat(source).containsExactly("a", "b", "a");
  }

  @Test
  void nullListReturnsNull() {
    assertThat(fn.execute(new Object[] { null }, null)).isNull();
  }
}
