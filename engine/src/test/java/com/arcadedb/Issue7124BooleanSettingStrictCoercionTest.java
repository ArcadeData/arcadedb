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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7124: a {@code Boolean} setting was the one type an administrative command could get wrong for free.
 * {@code GlobalConfiguration.coerce} answers {@code false} for anything it cannot read - deliberately, because it
 * runs inside that class's static initializer - so {@code SET SERVER SETTING ... requireAuthentication ture} was
 * stored as {@code Boolean.FALSE} and answered with a 200, and the text that produced it was gone before any reader
 * could tell the typo from a deliberate {@code false}.
 * <p>
 * {@link GlobalConfiguration#coerceFromAdminCommand(Object)} is the strict entry point the administrative paths use.
 * Its behaviour has to differ from {@code coerce} for exactly one type and be identical for every other.
 */
class Issue7124BooleanSettingStrictCoercionTest {

  private static final GlobalConfiguration BOOLEAN_SETTING = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

  @Test
  void aBooleanTypoIsRefusedRatherThanReadAsFalse() {
    // WHAT THE PERMISSIVE PATH DOES, AND MUST KEEP DOING: THE STATIC INITIALIZER CANNOT THROW.
    assertThat(BOOLEAN_SETTING.coerce("ture")).isEqualTo(Boolean.FALSE);

    assertThatThrownBy(() -> BOOLEAN_SETTING.coerceFromAdminCommand("ture")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(BOOLEAN_SETTING.getKey()).hasMessageContaining("ture");
  }

  @Test
  void everyOtherNonBooleanTextIsRefusedToo() {
    for (final String notABoolean : new String[] { "yes", "no", "1", "0", "on", "off", "" })
      assertThatThrownBy(() -> BOOLEAN_SETTING.coerceFromAdminCommand(notABoolean)).as(
          "'%s' is not a boolean and must not be folded to false", notABoolean).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void bothBooleanLiteralsAreAcceptedCaseInsensitivelyAndTrimmed() {
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("true")).isEqualTo(Boolean.TRUE);
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("false")).isEqualTo(Boolean.FALSE);
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(" TRUE ")).isEqualTo(Boolean.TRUE);
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("False")).isEqualTo(Boolean.FALSE);
  }

  @Test
  void anActualBooleanPassesThrough() {
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(Boolean.TRUE)).isEqualTo(Boolean.TRUE);
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(Boolean.FALSE)).isEqualTo(Boolean.FALSE);
    assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(null)).isNull();
  }

  @Test
  void everyOtherSettingTypeBehavesExactlyAsCoerceDoes() {
    // THE STRICT PATH ADDS A RULE FOR ONE TYPE AND MUST CHANGE NOTHING ELSE, INCLUDING WHAT IT ALREADY REFUSES.
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerceFromAdminCommand("12")).isEqualTo(12);
    assertThat(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerceFromAdminCommand("64GB")).isEqualTo(
        GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("64GB"));
    assertThat(GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT.coerceFromAdminCommand("ture")).isEqualTo("ture");

    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerceFromAdminCommand("abc")).isInstanceOf(
        NumberFormatException.class);
  }

  @Test
  void everyBooleanSettingGetsTheGuardNotJustThisOne() {
    // THE RULE IS THE TYPE'S, NOT ONE SETTING'S: A LATER Boolean SETTING INHERITS IT WITHOUT ANYONE REMEMBERING TO.
    int booleanSettings = 0;
    for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
      if (setting.getType() != Boolean.class)
        continue;

      booleanSettings++;
      assertThatThrownBy(() -> setting.coerceFromAdminCommand("ture")).as("setting '%s'", setting.getKey())
          .isInstanceOf(IllegalArgumentException.class);
    }

    assertThat(booleanSettings).isGreaterThan(1);
  }
}
