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

import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The io() permission check fails closed when the traversal is not bound to an ArcadeDB graph: without its database the
 * principal bound to the thread cannot be checked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArcadeIoRegistrationStrategyTest {

  @Test
  void ioOnANonArcadeGraphFailsClosed() {
    assertThatThrownBy(() -> ArcadeIoRegistrationStrategy.instance().apply(EmptyGraph.instance().traversal().io("x").asAdmin()))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Cannot verify the permission to use the io() step");
  }
}
