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
package com.arcadedb.utility;

/**
 * The wall clock at a {@link #RESOLUTION_MS} resolution, for the hot paths that only need to know roughly when
 * something happened: reading it is a volatile read, where {@link System#currentTimeMillis()} is a clock call on every
 * invocation - measurably slower, and on some platforms (macOS) one that stops scaling when many threads make it at
 * once (issue #8523: the database's "last used" stamp, taken on every schema lookup and record read, made a parallel
 * scan on 12 threads no faster than on one). One daemon thread per JVM keeps it current, started on first use.
 * <p>
 * Never use it to measure a duration or to enforce a deadline: it can lag the real clock by one resolution step.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CoarseClock {
  public static final long RESOLUTION_MS = 10;

  private static volatile long now = System.currentTimeMillis();

  static {
    final Thread ticker = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(RESOLUTION_MS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        now = System.currentTimeMillis();
      }
    }, "ArcadeDB-CoarseClock");
    ticker.setDaemon(true);
    ticker.start();
  }

  private CoarseClock() {
  }

  /** The wall clock in milliseconds, at most {@link #RESOLUTION_MS} behind {@link System#currentTimeMillis()}. */
  public static long currentTimeMillis() {
    return now;
  }
}
