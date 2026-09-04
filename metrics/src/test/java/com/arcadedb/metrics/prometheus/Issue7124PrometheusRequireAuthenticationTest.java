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
package com.arcadedb.metrics.prometheus;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the fourth finding of issue #7124: {@code /prometheus} used to read its
 * {@code requireAuthentication} flag with {@code Boolean.valueOf(configuration.getValue(key, "true"))}.
 * <p>
 * That had two defects. {@code ContextConfiguration.getValue(String, T)} infers {@code T} from the {@code "true"}
 * default and casts the stored value to {@code String}, so a value set programmatically as a {@code Boolean} threw
 * {@code ClassCastException}; and {@code Boolean.valueOf} answers {@code false} for anything it cannot parse, so a
 * typo such as {@code requireAuthentication=ture} FAILED OPEN and exposed the metrics endpoint unauthenticated.
 */
class Issue7124PrometheusRequireAuthenticationTest {

  private static final String KEY = "arcadedb.serverMetrics.prometheus.requireAuthentication";

  @AfterEach
  void clearSystemProperty() {
    System.clearProperty(KEY);
  }

  @Test
  void theSettingIsDeclaredAsABooleanDefaultingToTrue() {
    assertThat(GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getKey()).isEqualTo(KEY);
    assertThat(GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getDefValue()).isEqualTo(Boolean.TRUE);
  }

  @Test
  void anUnsetSettingRequiresAuthentication() {
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).isTrue();
  }

  @Test
  void aStringValueIsHonouredInBothDirections() {
    final ContextConfiguration configuration = new ContextConfiguration();

    configuration.setValue(KEY, "false");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isFalse();

    configuration.setValue(KEY, "true");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isTrue();
  }

  @Test
  void aStringValueIsReadCaseInsensitivelyAndTrimmed() {
    final ContextConfiguration configuration = new ContextConfiguration();

    configuration.setValue(KEY, "  FALSE ");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isFalse();

    configuration.setValue(KEY, " True");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isTrue();
  }

  @Test
  void aBooleanValueSetProgrammaticallyDoesNotThrow() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(KEY, Boolean.FALSE);

    assertThatCode(() -> PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).doesNotThrowAnyException();
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isFalse();

    configuration.setValue(KEY, Boolean.TRUE);
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isTrue();
  }

  @Test
  void anUnparseableValueFailsClosed() {
    final ContextConfiguration configuration = new ContextConfiguration();

    for (final String typo : new String[] { "ture", "yes", "1", "on", "", "  " }) {
      configuration.setValue(KEY, typo);
      assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).as(
          "'%s' is not a boolean: the endpoint must stay authenticated rather than silently open", typo).isTrue();
    }
  }

  @Test
  void theAdminCommandPathCannotSlipAPreCoercedTypoPast() {
    // THE GAP THE FIRST ROUND OF REVIEW FOUND. `SET SERVER SETTING <key> ture` AND THE set_server_setting MCP TOOL
    // BOTH CONVERT BEFORE STORING, SO A TYPO USED TO ARRIVE HERE AS Boolean.FALSE - INDISTINGUISHABLE FROM A
    // DELIBERATE false, WITH THE TEXT THAT PRODUCED IT ALREADY LOST. NO PARSE AT THIS END COULD HAVE CAUGHT IT;
    // THE REFUSAL HAS TO HAPPEN WHERE THE TEXT STILL EXISTS.
    final GlobalConfiguration setting = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

    assertThatThrownBy(() -> setting.coerceFromAdminCommand("ture")).isInstanceOf(IllegalArgumentException.class);

    // AND WHAT THAT PATH DOES STORE IS ALWAYS A VALUE THIS READER MAY TRUST.
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(KEY, setting.coerceFromAdminCommand("false"));
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isFalse();

    configuration.setValue(KEY, setting.coerceFromAdminCommand("true"));
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(configuration)).isTrue();
  }

  @Test
  void aSystemPropertyIsStillHonoured() {
    // THE EXISTING PLUGIN TESTS CONFIGURE THE FLAG THROUGH A SYSTEM PROPERTY, WHICH MUST KEEP WORKING.
    System.setProperty(KEY, "false");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).isFalse();

    System.setProperty(KEY, "ture");
    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).isTrue();
  }
}
