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
package com.arcadedb.query;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link QueryEngineManager#getAvailableLanguages()}: each registered
 * language alias appears exactly once. Two entries (e.g. {@code cypher} and {@code opencypher})
 * may point at the same factory instance, but each alias must still be surfaced as a distinct
 * entry rather than dropped or duplicated.
 */
class QueryEngineManagerLanguagesTest {

  @Test
  void availableLanguagesHaveNoDuplicates() {
    final List<String> languages = QueryEngineManager.getInstance().getAvailableLanguages();
    assertThat(languages).hasSameSizeAs(new HashSet<>(languages));
  }

  @Test
  void availableLanguagesIncludeBuiltInEngines() {
    final List<String> languages = QueryEngineManager.getInstance().getAvailableLanguages();
    // The four engines registered eagerly in the QueryEngineManager constructor are always
    // present regardless of which optional engines (opencypher, gremlin, mongo, etc.) are on
    // the classpath.
    assertThat(languages).contains("java", "sql", "sqlscript", "js");
  }

  @Test
  void availableLanguagesIncludeBothCypherAliases() {
    final List<String> languages = QueryEngineManager.getInstance().getAvailableLanguages();
    // OpenCypher lives in the engine module so the 'opencypher' factory is always registered.
    // Both 'opencypher' and 'cypher' aliases must surface as distinct list entries so callers
    // can probe either name; the factory instance behind both keys is the same, but the
    // registration must not collapse them.
    assertThat(languages).contains("opencypher", "cypher");
  }
}
