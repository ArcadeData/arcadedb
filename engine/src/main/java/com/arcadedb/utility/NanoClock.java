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

package com.arcadedb.utility;

import java.time.*;

public class NanoClock extends Clock {
  private final Clock   clock;
  private final Instant beginInstant;
  private final long    beginNanos;

  public NanoClock() {
    this(Clock.systemUTC());
  }

  public NanoClock(final Clock clock) {
    this.clock = clock;
    beginInstant = clock.instant();
    beginNanos = getSystemNanos();
  }

  @Override
  public ZoneId getZone() {
    return clock.getZone();
  }

  @Override
  public Instant instant() {
    return beginInstant.plusNanos(getSystemNanos() - beginNanos);
  }

  @Override
  public Clock withZone(final ZoneId zone) {
    return new NanoClock(clock.withZone(zone));
  }

  private long getSystemNanos() {
    return System.nanoTime();
  }
}
