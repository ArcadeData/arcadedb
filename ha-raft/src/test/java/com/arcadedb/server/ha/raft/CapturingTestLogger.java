/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * Test helper that captures every ArcadeDB LogManager message across all in-process servers by
 * replacing the global logger for the duration of a test. Thread-safe: the in-process HA servers log
 * from many threads concurrently. It captures the raw message template (before argument substitution),
 * which is sufficient for substring assertions because every asserted phrase is a literal in the format
 * string. It does NOT capture Apache Ratis's own java.util.logging output - use a JUL Handler for that.
 */
final class CapturingTestLogger implements Logger {

  private final List<String> messages = new CopyOnWriteArrayList<>();

  static CapturingTestLogger install() {
    final CapturingTestLogger logger = new CapturingTestLogger();
    LogManager.instance().setLogger(logger);
    return logger;
  }

  void uninstall() {
    LogManager.instance().setLogger(new DefaultLogger());
  }

  int countContaining(final String... needles) {
    int n = 0;
    for (final String m : messages) {
      boolean all = true;
      for (final String needle : needles)
        if (!m.contains(needle)) {
          all = false;
          break;
        }
      if (all)
        n++;
    }
    return n;
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
      final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
      final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    if (iMessage != null)
      messages.add(iMessage);
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object... args) {
    if (iMessage != null)
      messages.add(iMessage);
  }

  @Override
  public void flush() {
  }
}
