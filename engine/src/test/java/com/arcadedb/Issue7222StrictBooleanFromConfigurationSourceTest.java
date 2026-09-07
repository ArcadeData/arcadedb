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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7222.
 * <p>
 * {@code arcadedb.serverMetrics.prometheus.requireAuthentication} is documented as having a STRICT parse so that "a
 * typo cannot silently publish the endpoint unauthenticated". That held on the two administrative writers and not on
 * the third one - {@link GlobalConfiguration#readConfiguration()}, which feeds every system property and environment
 * variable through the PERMISSIVE {@code coerce}, where {@code Boolean.parseBoolean} maps everything that is not
 * {@code "true"} to {@code false} with no error.
 * <p>
 * So {@code requireAuthentication=yes} in a Kubernetes env var stored {@code Boolean.FALSE}, the plugin's own strict
 * check found a value that was already a {@code Boolean} and had nothing left to reject, and {@code /prometheus} was
 * published unauthenticated while the operator believed they had turned the protection ON. The refusal has to happen
 * where the text still exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7222StrictBooleanFromConfigurationSourceTest {

  private static final GlobalConfiguration BOOLEAN_SETTING_DEFAULTING_TO_TRUE  =
      GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;
  private static final GlobalConfiguration BOOLEAN_SETTING_DEFAULTING_TO_FALSE = GlobalConfiguration.NETWORK_USE_SSL;

  @AfterEach
  void restoreTheSettings() {
    BOOLEAN_SETTING_DEFAULTING_TO_TRUE.reset();
    BOOLEAN_SETTING_DEFAULTING_TO_FALSE.reset();
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
  }

  @Test
  void aValueThatIsNeitherTrueNorFalseIsRefusedRatherThanReadAsFalse() {
    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getDefValue()).isEqualTo(Boolean.TRUE);

    for (final String typo : new String[] { "yes", "1", "on", "TRUE!", "ture", "" }) {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(typo, "system property")).as(
          "'%s' was accepted", typo).isFalse();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s' turned the setting OFF", typo).isTrue();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.isChanged()).as("'%s' was recorded as an explicit choice", typo)
          .isFalse();
    }
  }

  /**
   * The refusal keeps the DEFAULT rather than forcing {@code true}: what makes this setting fail closed is that its
   * own default is the safe value, and a general rule that read every unparseable boolean as {@code true} would flip
   * settings whose safe side is the other one. Refusing to guess is the property that has to hold for all of them.
   */
  @Test
  void aRefusedValueKeepsTheSettingsOwnDefaultInBothDirections() {
    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.getDefValue()).isEqualTo(Boolean.FALSE);

    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.setValueFromConfigurationSource("yes", "environment variable"))
        .isFalse();
    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.getValueAsBoolean()).isFalse();
  }

  @Test
  void trueAndFalseAreStillHonouredInEveryCasingAndWithSurroundingSpace() {
    for (final String text : new String[] { "false", "FALSE", "  False  " }) {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(text, "system property")).isTrue();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s'", text).isFalse();
    }

    for (final String text : new String[] { "true", "TRUE", " True " }) {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(text, "system property")).isTrue();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s'", text).isTrue();
    }
  }

  /**
   * Only {@code Boolean} settings gain the strict parse. Every other type already refuses what it cannot read, and
   * the leniency this path needs is about not taking the engine down over one bad value - not about accepting one.
   */
  @Test
  void aNonBooleanSettingIsUnaffected() {
    assertThat(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValueFromConfigurationSource("/mnt/data", "system property"))
        .isTrue();
    assertThat(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString()).isEqualTo("/mnt/data");
  }

  /**
   * An unreadable value of a non-Boolean setting must not take the whole engine down: this method runs inside this
   * class's static initializer, where a throw becomes an {@code ExceptionInInitializerError}. It is reported and the
   * setting keeps its default, which is the same answer the Boolean arm gives.
   */
  @Test
  void anUnreadableValueOfAnyTypeIsReportedRatherThanThrown() {
    final int previous = GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger();

    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.setValueFromConfigurationSource("abc", "environment variable"))
        .isFalse();
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger()).isEqualTo(previous);
  }

  /** A value that arrives already typed - a callback's return, a programmatic write - is not text and is stored. */
  @Test
  void anAlreadyTypedValueIsStored() {
    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(Boolean.FALSE, "system property"))
        .isTrue();
    assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).isFalse();
  }
}
