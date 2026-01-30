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
package com.arcadedb.query.polyglot;

import org.graalvm.polyglot.Engine;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for PolyglotEngineManager to ensure engine pooling works correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PolyglotEngineManagerTest {

  @Test
  void sharedEngineIsSingleton() {
    final PolyglotEngineManager manager = PolyglotEngineManager.getInstance();
    assertThat(manager).as("PolyglotEngineManager instance should not be null").isNotNull();

    // Get the shared engine multiple times
    final Engine engine1 = manager.getSharedEngine();
    final Engine engine2 = manager.getSharedEngine();
    final Engine engine3 = manager.getSharedEngine();

    assertThat(engine1).as("First engine instance should not be null").isNotNull();
    assertThat(engine2).as("Second engine instance should not be null").isNotNull();
    assertThat(engine3).as("Third engine instance should not be null").isNotNull();

    // Verify all references point to the same instance (singleton)
    assertThat(engine2).as("Engine instances should be the same object (singleton)").isSameAs(engine1);
    assertThat(engine3).as("Engine instances should be the same object (singleton)").isSameAs(engine2);
    assertThat(engine3).as("Engine instances should be the same object (singleton)").isSameAs(engine1);
  }

  @Test
  void multipleManagerInstancesShareSameEngine() {
    // Even though we get multiple manager instances (singleton), they should return the same engine
    final PolyglotEngineManager manager1 = PolyglotEngineManager.getInstance();
    final PolyglotEngineManager manager2 = PolyglotEngineManager.getInstance();

    assertThat(manager2).as("Manager instances should be the same (singleton)").isSameAs(manager1);

    final Engine engine1 = manager1.getSharedEngine();
    final Engine engine2 = manager2.getSharedEngine();

    assertThat(engine2).as("Engines from different manager instances should be the same").isSameAs(engine1);
  }
}
