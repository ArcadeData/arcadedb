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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbstractServerHttpHandler#generateCorrelationId()}: the cheap non-cryptographic
 * request-correlation id (used when the client sends no X-Request-Id) must be non-blank, unique across
 * calls, safely bounded, and printable so {@code sanitizeRequestId} echoes it unchanged.
 */
class CorrelationIdGenerationTest {

  @Test
  void generatesNonBlankIds() {
    final String id = AbstractServerHttpHandler.generateCorrelationId();
    assertThat(id).isNotNull().isNotBlank();
  }

  @Test
  void generatedIdsAreUnique() {
    final Set<String> ids = new HashSet<>();
    for (int i = 0; i < 10_000; i++)
      ids.add(AbstractServerHttpHandler.generateCorrelationId());
    assertThat(ids).hasSize(10_000);
  }

  @Test
  void generatedIdContainsOnlyHexAndSeparator() {
    final String id = AbstractServerHttpHandler.generateCorrelationId();
    assertThat(id).matches("[0-9a-f]+-[0-9a-f]+");
  }

  @Test
  void generatedIdSurvivesSanitizationUnchanged() {
    // The generated id is echoed and logged through sanitizeRequestId; it must be short and printable
    // enough that sanitization returns it verbatim.
    final String id = AbstractServerHttpHandler.generateCorrelationId();
    assertThat(id).hasSizeLessThanOrEqualTo(128);
    assertThat(AbstractServerHttpHandler.sanitizeRequestId(id)).isEqualTo(id);
  }
}
