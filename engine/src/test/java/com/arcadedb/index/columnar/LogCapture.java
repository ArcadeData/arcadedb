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
package com.arcadedb.index.columnar;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Collects every message logged while it is installed, at every level and formatted, and forwards each to the logger it replaced.
 * Installed by the constructor and removed by {@link #close()}, so a test holds it in try-with-resources.
 */
public final class LogCapture implements Logger, AutoCloseable {
  /** One logged message. */
  public record Entry(Level level, String message) {
  }

  private final Logger      previous;
  private final List<Entry> entries = new ArrayList<>();

  public LogCapture() {
    previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(this);
  }

  /** The messages at exactly {@code level} that contain {@code text}, in logging order. */
  public synchronized List<String> messages(final Level level, final String text) {
    final List<String> result = new ArrayList<>();
    for (final Entry entry : entries)
      if (entry.level() == level && entry.message().contains(text))
        result.add(entry.message());
    return result;
  }

  @Override
  public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
      final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
      final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
      final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    capture(level, message,
        new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
    previous.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
        arg12, arg13, arg14, arg15, arg16, arg17);
  }

  @Override
  public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
      final Object... args) {
    capture(level, message, args);
    previous.log(requester, level, message, exception, context, args);
  }

  @Override
  public void flush() {
    previous.flush();
  }

  @Override
  public void close() {
    LogManager.instance().setLogger(previous);
  }

  private synchronized void capture(final Level level, final String message, final Object[] args) {
    if (message != null)
      entries.add(new Entry(level, args == null || args.length == 0 ? message : String.format(message, args)));
  }
}
