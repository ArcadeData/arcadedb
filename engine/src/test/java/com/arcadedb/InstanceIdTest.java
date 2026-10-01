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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceIdTest {
  private static final String VALID = "adb-123e4567-e89b-12d3-a456-426614174000";

  @Test
  void validId() {
    assertThat(InstanceId.isValid(VALID)).isTrue();
    assertThat(VALID).hasSize(40);
    assertThat(InstanceId.normalize(VALID)).isEqualTo(VALID);
  }

  @Test
  void upperCaseAndWhitespaceAreNormalized() {
    assertThat(InstanceId.normalize("  ADB-123E4567-E89B-12D3-A456-426614174000\n")).isEqualTo(VALID);
    assertThat(InstanceId.isValid(VALID.toUpperCase())).isFalse();
  }

  @Test
  void invalidValuesAreRejected() {
    assertThat(InstanceId.normalize("adb-123e4567-e89b-12d3-a456-42661417400")).isNull();
    assertThat(InstanceId.normalize("xyz-123e4567-e89b-12d3-a456-426614174000")).isNull();
    assertThat(InstanceId.normalize("123e4567-e89b-12d3-a456-426614174000")).isNull();
    assertThat(InstanceId.normalize("adb-123e4567-e89b-12d3-a456-42661417400g")).isNull();
    assertThat(InstanceId.normalize(VALID + "0")).isNull();
    assertThat(InstanceId.normalize(VALID + " x")).isNull();
    assertThat(InstanceId.normalize("")).isNull();
    assertThat(InstanceId.normalize("   ")).isNull();
    assertThat(InstanceId.normalize(null)).isNull();
    assertThat(InstanceId.isValid(null)).isFalse();
    assertThat(InstanceId.isValid("")).isFalse();
  }

  @Test
  void generatedIdIsValidAndUnique() {
    final String a = InstanceId.generate();
    assertThat(InstanceId.isValid(a)).isTrue();
    assertThat(a).hasSize(40);
    assertThat(InstanceId.generate()).isNotEqualTo(a);
  }
}
