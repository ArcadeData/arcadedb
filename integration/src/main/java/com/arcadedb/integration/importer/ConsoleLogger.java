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
package com.arcadedb.integration.importer;

/**
 * Console logger used by importers and exporters. Supports an optional {@link LogListener}
 * for structured progress reporting (e.g., SSE streaming to clients).
 *
 * @author Luca Garulli
 */
public class ConsoleLogger {
  private final int          verboseLevel;
  /**
   * Serializes the notifications, because a listener is written once and called from wherever the logger is used, and
   * since #6086 that includes several threads at a time: a parallel restore logs one line per archive entry from its
   * worker pool. The listeners in the tree write those lines to a stream nobody synchronizes - the server's SSE
   * progress channel writes straight to the exchange's output stream - so two lines arriving at once would interleave
   * into a corrupt event rather than merely arriving out of order. Serializing here keeps that guarantee in one place
   * instead of asking every implementation of a one-method interface to remember it.
   */
  private final Object       listenerLock = new Object();
  private volatile LogListener listener;

  @FunctionalInterface
  public interface LogListener {
    void onLogLine(String message);
  }

  public ConsoleLogger(final int verboseLevel) {
    this.verboseLevel = verboseLevel;
  }

  public ConsoleLogger(final int verboseLevel, final LogListener listener) {
    this.verboseLevel = verboseLevel;
    this.listener = listener;
  }

  public void logLine(final int level, final String text, final Object... args) {
    if (level > verboseLevel)
      return;

    final String msg = args.length == 0 ? text : text.formatted(args);
    System.out.println(msg);
    notifyListener(msg);
  }

  public void log(final int level, final String text, final Object... args) {
    if (level > verboseLevel)
      return;

    if (args.length == 0)
      System.out.print(text);
    else
      System.out.print(text.formatted(args));
  }

  public void errorLine(final String text, final Object... args) {
    final String msg = args.length == 0 ? text : text.formatted(args);
    System.out.println(msg);
    notifyListener(msg);
  }

  private void notifyListener(final String msg) {
    final LogListener current = listener;
    if (current == null)
      return;
    synchronized (listenerLock) {
      current.onLogLine(msg);
    }
  }

  public int getVerboseLevel() {
    return verboseLevel;
  }

  public void setListener(final LogListener listener) {
    this.listener = listener;
  }
}
