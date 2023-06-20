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
package com.arcadedb.server.monitor;

import java.util.*;

public interface ServerMetrics {
  interface Meter {
    void hit();

    float getRequestsPerSecondInLastMinute();

    float getRequestsPerSecondSinceLastAsked();

    long getTotalRequestsInLastMinute();

    long getTotalCounter();
  }

  void stop();

  Map<String, Meter> getMeters();

  Meter meter(String name);
}
