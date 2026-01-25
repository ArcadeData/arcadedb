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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ResultInternal}.
 */
class ResultInternalTest {

  @Test
  void similarityProperty() {
    final ResultInternal result = new ResultInternal();
    result.setSimilarity(0.85f);

    assertThat(result.getSimilarity()).isEqualTo(0.85f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0.85f);
    assertThat(result.hasProperty("$similarity")).isTrue();
    assertThat(result.getPropertyNames()).contains("$similarity");
  }

  @Test
  void similarityDefaultsToZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.getSimilarity()).isEqualTo(0f);
    assertThat(result.<Float>getProperty("$similarity")).isEqualTo(0f);
  }

  @Test
  void similarityNotInPropertyNamesWhenZero() {
    final ResultInternal result = new ResultInternal();

    assertThat(result.hasProperty("$similarity")).isTrue(); // $similarity is always available
    assertThat(result.getPropertyNames()).doesNotContain("$similarity"); // but not in names when 0
  }

  @Test
  void similarityBoundaryValues() {
    final ResultInternal result = new ResultInternal();

    // Test minimum value
    result.setSimilarity(0.0f);
    assertThat(result.getSimilarity()).isEqualTo(0.0f);

    // Test maximum value
    result.setSimilarity(1.0f);
    assertThat(result.getSimilarity()).isEqualTo(1.0f);
    assertThat(result.getPropertyNames()).contains("$similarity");

    // Test intermediate value
    result.setSimilarity(0.5f);
    assertThat(result.getSimilarity()).isEqualTo(0.5f);
  }
}
