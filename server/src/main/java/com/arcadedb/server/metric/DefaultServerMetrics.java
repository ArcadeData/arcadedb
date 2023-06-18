/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.metric;

import java.util.*;
import java.util.concurrent.*;

/**
 * Stores the metrics in RAM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DefaultServerMetrics implements ServerMetrics {
  protected ConcurrentHashMap<String, Meter> metricsRegistry;

  public DefaultServerMetrics() {
    metricsRegistry = new ConcurrentHashMap<>();
  }

  @Override
  public void stop() {
    metricsRegistry = null;
  }

  @Override
  public Meter meter(final String name) {
    return metricsRegistry.computeIfAbsent(name, k -> new MetricMeter());
  }

  @Override
  public Map<String, Meter> getMeters() {
    return metricsRegistry;
  }
}
