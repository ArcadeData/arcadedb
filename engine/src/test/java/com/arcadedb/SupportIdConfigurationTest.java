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

/**
 * {@link GlobalConfiguration#SUPPORT_ID} accepts only canonical UUIDs (any case) and ignores anything else.
 */
class SupportIdConfigurationTest {

  private static final String UUID_LOWER = "123e4567-e89b-12d3-a456-426614174000";

  private static String configured(final String value) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SUPPORT_ID, value);
    return cfg.getValueAsString(GlobalConfiguration.SUPPORT_ID);
  }

  @Test
  void defaultIsEmpty() {
    assertThat(new ContextConfiguration().getValueAsString(GlobalConfiguration.SUPPORT_ID)).isEmpty();
    assertThat(GlobalConfiguration.SUPPORT_ID.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
  }

  @Test
  void validUuidIsAccepted() {
    assertThat(configured(UUID_LOWER)).isEqualTo(UUID_LOWER);
  }

  @Test
  void upperCaseUuidIsAccepted() {
    assertThat(configured(UUID_LOWER.toUpperCase())).isEqualTo(UUID_LOWER.toUpperCase());
  }

  @Test
  void emptyIsAccepted() {
    assertThat(configured("")).isEmpty();
  }

  @Test
  void malformedIsIgnored() {
    assertThat(configured("not-a-uuid")).isEmpty();
    assertThat(configured("123e4567e89b12d3a456426614174000")).isEmpty();
    assertThat(configured("123e4567-e89b-12d3-a456-42661417400")).isEmpty();
    assertThat(configured("123e4567-e89b-12d3-a456-42661417400g")).isEmpty();
  }

  @Test
  void validatorHelper() {
    assertThat(GlobalConfiguration.isValidSupportId(UUID_LOWER)).isTrue();
    assertThat(GlobalConfiguration.isValidSupportId(null)).isFalse();
    assertThat(GlobalConfiguration.isValidSupportId("x")).isFalse();
  }
}
