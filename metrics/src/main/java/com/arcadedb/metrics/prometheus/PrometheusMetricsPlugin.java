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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.undertow.server.handlers.PathHandler;

import java.util.logging.Level;

public class PrometheusMetricsPlugin implements ServerPlugin {

  private PrometheusMeterRegistry registry;
  private boolean                 enabled;
  private ContextConfiguration    configuration;

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
    enabled = configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS);
    this.configuration = configuration;
    if (!enabled)
      return;
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    //Add to global metrics registry
    Metrics.addRegistry(registry);
  }

  @Override
  public void startService() {
    if (enabled) {
      LogManager.instance().log(this, Level.INFO, "Prometheus backend metrics enabled");
    }
  }

  @Override
  public void stopService() {
    if (registry != null) {
      Metrics.removeRegistry(registry);
      registry.close();
      registry = null;
    }
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    if (!enabled)
      return;

    routes.addExactPath("/prometheus", new GetPrometheusMetricsHandler(httpServer, registry, isAuthenticationRequired(configuration)));

    LogManager.instance().log(this, Level.INFO, "Prometheus backend metrics http handler configured");

  }

  /**
   * Reads {@link GlobalConfiguration#SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION} and FAILS CLOSED.
   * <p>
   * Issue #7124: this used to be {@code Boolean.valueOf(configuration.getValue(key, "true"))}, which had two
   * defects. {@link ContextConfiguration#getValue(String, Object)} infers its type parameter from the {@code "true"}
   * default and casts the stored value to {@link String}, so a value set programmatically as a {@link Boolean} threw
   * {@code ClassCastException}; and {@code Boolean.valueOf} answers {@code false} for anything it cannot parse, so
   * {@code requireAuthentication=ture} silently published the metrics endpoint unauthenticated.
   * <p>
   * The strict parse lives here rather than in {@link GlobalConfiguration#coerce(Object)}, which stays permissive on
   * purpose: it runs inside that class's static initializer over every system property and environment variable, so
   * a throw there becomes an {@code ExceptionInInitializerError} that takes the whole engine down instead of the
   * setting. An authentication switch is worth the extra care at the one site that reads it.
   * <p>
   * A value that arrives ALREADY a {@link Boolean} is trusted, and that is only sound because the two paths that
   * store one - {@code SET SERVER SETTING} and the {@code set_server_setting} MCP tool - both go through
   * {@link GlobalConfiguration#coerceFromAdminCommand(Object)}, which refuses a boolean it cannot read rather than
   * folding it to {@code false}. Were either to fall back to the permissive {@code coerce}, a typo would reach here
   * as {@code Boolean.FALSE} with the text that produced it already lost, and no parse at this end could tell it
   * from a deliberate {@code false}.
   *
   * @return {@code true} unless the configured value is unambiguously {@code false}
   */
  static boolean isAuthenticationRequired(final ContextConfiguration configuration) {
    final String key = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getKey();

    // READ AS Object: THE CONTEXT MAP CAN HOLD EITHER THE RAW STRING FROM THE SERVER CONFIGURATION OR A Boolean SET
    // PROGRAMMATICALLY, AND getValue() BLINDLY CASTS TO THE TYPE OF THE DEFAULT IT IS GIVEN.
    Object value = configuration != null ? configuration.getValue(key, (Object) null) : null;
    if (value == null)
      value = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getValue();

    // ONE DEFINITION OF WHAT COUNTS AS BOOLEAN TEXT, SHARED WITH THE WRITE SITES: A SECOND COPY HERE COULD DRIFT
    // FROM THEIRS AND REOPEN THIS BUG ON WHICHEVER SIDE FELL BEHIND. THE ONLY DIFFERENCE IS THE ANSWER TO A VALUE
    // NEITHER CAN READ - THEY REFUSE THE COMMAND, THIS ONE CANNOT REFUSE A SERVER STARTUP, SO IT FAILS CLOSED.
    try {
      final Object coerced = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.coerceFromAdminCommand(value);
      return !(coerced instanceof Boolean b) || b;
    } catch (final RuntimeException e) {
      LogManager.instance().log(PrometheusMetricsPlugin.class, Level.WARNING,
          "Invalid value '%s' for setting '%s': only 'true' and 'false' are accepted. Requiring authentication on the /prometheus endpoint",
          value, key);
      return true;
    }
  }

}
