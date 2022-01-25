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
package com.arcadedb.server;

import com.arcadedb.Constants;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;

public class JMXServerMetrics implements ServerMetrics {
  private       MetricRegistry metricsRegistry;
  private final JmxReporter    jmxReporter;

  public JMXServerMetrics() {
    metricsRegistry = new MetricRegistry();
    jmxReporter = JmxReporter.forRegistry(metricsRegistry).inDomain(Constants.PRODUCT).build();
    jmxReporter.start();
  }

  @Override
  public void stop() {
    jmxReporter.stop();
    metricsRegistry = null;
  }

  @Override
  public MetricTimer timer(final String name) {
    final Timer.Context t = metricsRegistry.timer(name).time();
    return () -> t.stop();
  }

  @Override
  public MetricMeter meter(final String name) {
    final Meter m = metricsRegistry.meter(name);
    return () -> m.mark();
  }
}
