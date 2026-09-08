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

/**
 * Regression test for GitHub issue #7159: {@code /prometheus} kept the authentication requirement captured
 * when the route was registered, so {@code SET SERVER SETTING
 * arcadedb.serverMetrics.prometheus.requireAuthentication ...} was answered with a 200, stored the new value,
 * and changed nothing about the live endpoint until the next server restart.
 * <p>
 * The route now asks the configuration per request. Everything below drives the handler's
 * {@code isRequireAuthentication()} - the method {@code AbstractServerHttpHandler} calls on each request -
 * against the very {@link ContextConfiguration} the admin commands write into.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7159PrometheusRequireAuthenticationLiveTest {
  private static final String KEY = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getKey();

  private final ContextConfiguration          configuration = new ContextConfiguration();
  private final GetPrometheusMetricsHandler   handler       = new GetPrometheusMetricsHandler(null, null, configuration);

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.reset();
  }

  @Test
  void loosenedAtRuntimeTheEndpointOpens() {
    configuration.setValue(KEY, true);
    assertThat(handler.isRequireAuthentication()).isTrue();

    configuration.setValue(KEY, false);
    assertThat(handler.isRequireAuthentication()).as("the live route follows the setting").isFalse();
  }

  /** The asymmetric half of #7159: an operator closing the endpoint must actually close it. */
  @Test
  void tightenedAtRuntimeTheEndpointCloses() {
    configuration.setValue(KEY, false);
    assertThat(handler.isRequireAuthentication()).isFalse();

    configuration.setValue(KEY, true);
    assertThat(handler.isRequireAuthentication()).isTrue();
  }

  /** The admin commands store text, not a Boolean, and the route has to follow that spelling too. */
  @Test
  void theTextualSpellingsAreFollowedAsWell() {
    configuration.setValue(KEY, "false");
    assertThat(handler.isRequireAuthentication()).isFalse();

    configuration.setValue(KEY, "true");
    assertThat(handler.isRequireAuthentication()).isTrue();
  }

  /** With nothing in the overlay the enum is authoritative, and a change to it is followed just the same. */
  @Test
  void aChangeToTheGlobalSettingIsFollowedWhenTheOverlayIsEmpty() {
    GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.setValue(false);
    assertThat(handler.isRequireAuthentication()).isFalse();

    GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.setValue(true);
    assertThat(handler.isRequireAuthentication()).isTrue();
  }

  /** A value neither {@code true} nor {@code false} still fails closed, on every request. */
  @Test
  void anUnreadableValueKeepsFailingClosed() {
    configuration.setValue(KEY, false);
    assertThat(handler.isRequireAuthentication()).isFalse();

    configuration.setValue(KEY, "ture");
    assertThat(handler.isRequireAuthentication()).isTrue();
    assertThat(handler.isRequireAuthentication()).as("and on the scrape after it, from the cached decision").isTrue();
  }
}
