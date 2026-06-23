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
package com.arcadedb.remote;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RemoteGraphBatch} reference resolution that do not require a running server.
 * The vertex reference is validated before any buffering or network interaction, so a null/empty
 * reference must be rejected with a clear {@link IllegalArgumentException} instead of an obscure
 * NullPointerException / StringIndexOutOfBoundsException.
 */
class RemoteGraphBatchTest {

  private RemoteGraphBatch newBatch() {
    // database is never touched: resolveRef() runs before any buffering/flush.
    return new RemoteGraphBatch(null, new HashMap<>(), Integer.MAX_VALUE);
  }

  @Test
  void createEdgeRejectsNullFromReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", (String) null, "#3:0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsNullToReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "#3:0", (String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsEmptyFromReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "", "#3:0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void createEdgeRejectsEmptyToReference() {
    final RemoteGraphBatch batch = newBatch();
    assertThatThrownBy(() -> batch.createEdge("KNOWS", "#3:0", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }
}
