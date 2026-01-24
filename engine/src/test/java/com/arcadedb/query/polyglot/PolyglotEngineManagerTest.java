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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for PolyglotEngineManager to ensure engine pooling works correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PolyglotEngineManagerTest {

  @Test
  public void testSharedEngineIsSingleton() {
    final PolyglotEngineManager manager = PolyglotEngineManager.getInstance();
    Assertions.assertNotNull(manager, "PolyglotEngineManager instance should not be null");

    // Get the shared engine multiple times
    final Engine engine1 = manager.getSharedEngine();
    final Engine engine2 = manager.getSharedEngine();
    final Engine engine3 = manager.getSharedEngine();

    Assertions.assertNotNull(engine1, "First engine instance should not be null");
    Assertions.assertNotNull(engine2, "Second engine instance should not be null");
    Assertions.assertNotNull(engine3, "Third engine instance should not be null");

    // Verify all references point to the same instance (singleton)
    Assertions.assertSame(engine1, engine2, "Engine instances should be the same object (singleton)");
    Assertions.assertSame(engine2, engine3, "Engine instances should be the same object (singleton)");
    Assertions.assertSame(engine1, engine3, "Engine instances should be the same object (singleton)");
  }

  @Test
  public void testMultipleManagerInstancesShareSameEngine() {
    // Even though we get multiple manager instances (singleton), they should return the same engine
    final PolyglotEngineManager manager1 = PolyglotEngineManager.getInstance();
    final PolyglotEngineManager manager2 = PolyglotEngineManager.getInstance();

    Assertions.assertSame(manager1, manager2, "Manager instances should be the same (singleton)");

    final Engine engine1 = manager1.getSharedEngine();
    final Engine engine2 = manager2.getSharedEngine();

    Assertions.assertSame(engine1, engine2, "Engines from different manager instances should be the same");
  }
}
