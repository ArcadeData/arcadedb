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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7222, at the endpoint the defect actually reached.
 * <p>
 * #7124 made the plugin fail closed on a value it cannot read, but it can only do that while the TEXT still exists.
 * The system-property and environment-variable path used the permissive coercion, so
 * {@code requireAuthentication=yes} was stored as {@code Boolean.FALSE} and arrived here already typed - and a value
 * that is already a {@code Boolean} is exactly the one the strict guard skips, because a deliberate {@code false} is
 * indistinguishable from a folded typo. {@code /prometheus} was then published unauthenticated, silently, on the one
 * configuration path a Kubernetes deployment uses.
 * <p>
 * The environment-variable half is what these tests exercise: unlike a system property, an environment variable is
 * not re-read by {@link ContextConfiguration#getValue(String, Object)}, so nothing downstream can recover the text
 * and the process-wide value is all the plugin sees. Setting an environment variable is not portable from a test, so
 * the entry point {@code readConfiguration()} calls is invoked directly - it is the same call with the same argument.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7222PrometheusEnvironmentPathTest {

  private static final GlobalConfiguration SETTING = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

  private String previousSystemProperty;

  /**
   * The environment path is the one where the raw text is NOT recoverable downstream, so the system property has to
   * be out of the way: {@link ContextConfiguration#getValue(String, Object)} re-reads it and would answer with the
   * text instead of the stored value. Other test classes in this module set it and leave it set.
   */
  @BeforeEach
  void hideTheSystemProperty() {
    previousSystemProperty = System.getProperty(SETTING.getKey());
    System.clearProperty(SETTING.getKey());
  }

  @AfterEach
  void restoreTheSetting() {
    SETTING.reset();
    if (previousSystemProperty != null)
      System.setProperty(SETTING.getKey(), previousSystemProperty);
  }

  @Test
  void aTypoInTheEnvironmentDoesNotPublishTheEndpointUnauthenticated() {
    for (final String typo : new String[] { "yes", "1", "on", "TRUE!", "ture" }) {
      SETTING.setValueFromConfigurationSource(typo, "environment variable");

      assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).as(
          "'%s' published /prometheus unauthenticated", typo).isTrue();
    }
  }

  @Test
  void aDeliberateFalseFromTheEnvironmentIsStillHonoured() {
    SETTING.setValueFromConfigurationSource("false", "environment variable");

    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).isFalse();
  }

  @Test
  void anExplicitTrueFromTheEnvironmentIsHonoured() {
    SETTING.setValueFromConfigurationSource("TRUE", "environment variable");

    assertThat(PrometheusMetricsPlugin.isAuthenticationRequired(new ContextConfiguration())).isTrue();
  }
}
