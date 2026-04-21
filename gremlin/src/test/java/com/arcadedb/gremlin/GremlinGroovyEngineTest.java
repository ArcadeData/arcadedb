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
package com.arcadedb.gremlin;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.CoreGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Gremlin Groovy script engine initialises correctly with the TinkerPop
 * core imports.  With Groovy 5 (incompatible with TinkerPop 3.8.x) this fails with
 * "The name valueOf is already declared" because {@code ImportGroovyCustomizer} adds a
 * wildcard static import ({@code import static Enum.*}) for every enum in
 * {@link org.apache.tinkerpop.gremlin.jsr223.CoreImports}, and Groovy 5 rejects duplicate
 * {@code valueOf} symbols introduced by multiple such imports.
 *
 * <p>Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/3724">#3724</a>.
 */
class GremlinGroovyEngineTest {

  @Test
  void groovyEngineInitializesWithCoreImports() throws Exception {
    final Customizer[] customizers = CoreGremlinPlugin.instance()
        .getCustomizers("gremlin-groovy")
        .orElse(new Customizer[0]);

    final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customizers);

    // A successful eval proves the engine started without "valueOf is already declared"
    assertThat(engine.eval("1 + 1")).isEqualTo(2);
  }
}
