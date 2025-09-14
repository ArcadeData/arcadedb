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
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    if (!enabled)
      return;

    Boolean requireAuthentication = Boolean.valueOf(configuration.getValue("arcadedb.serverMetrics.prometheus.requireAuthentication", "true"));
    routes.addExactPath("/prometheus", new GetPrometheusMetricsHandler(httpServer, registry, requireAuthentication));

    LogManager.instance().log(this, Level.INFO, "Prometheus backend metrics http handler configured");

  }

}
