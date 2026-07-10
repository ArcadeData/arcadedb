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
package com.arcadedb.query.opencypher.executor.steps;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CypherStatisticsHelperTest {

  @Test
  void countsPreExistingPropertiesNotReSetAndSkipsInternal() {
    final Set<String> existing = Set.of(
        "a",
        "b",
        "@type"); // internal - never counted
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", 1); // re-set, not a removal
    // b is removed (absent from replacement) -> counts 1
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }

  @Test
  void nullValuedReplacementIsARemoval() {
    final Set<String> existing = new LinkedHashSet<>();
    existing.add("a");
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", null); // map.get("a") == null -> counts as removed
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }
}
