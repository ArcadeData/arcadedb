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
  private final int         verboseLevel;
  private       LogListener listener;

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
    if (listener != null)
      listener.onLogLine(msg);
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
    if (listener != null)
      listener.onLogLine(msg);
  }

  public int getVerboseLevel() {
    return verboseLevel;
  }

  public void setListener(final LogListener listener) {
    this.listener = listener;
  }
}
