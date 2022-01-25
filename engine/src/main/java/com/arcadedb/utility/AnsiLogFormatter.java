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

import static java.util.logging.Level.SEVERE;

import com.arcadedb.log.LogFormatter;

import java.util.Date;
import java.util.IllegalFormatException;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Log formatter that uses ANSI code if they are available and enabled.
 *
 * @author Luca Garulli
 */
public class AnsiLogFormatter extends LogFormatter {

  @Override
  protected String customFormatMessage(final LogRecord iRecord) {
    final Level level = iRecord.getLevel();
    final String message = AnsiCode.format(iRecord.getMessage());
    final Object[] additionalArgs = iRecord.getParameters();
    final String requester = getSourceClassSimpleName(iRecord.getLoggerName());

    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(EOL);

    if (AnsiCode.isSupportsColors())
      buffer.append("$ANSI{cyan ");
    synchronized (dateFormat) {
      buffer.append(dateFormat.format(new Date()));
    }

    if (AnsiCode.isSupportsColors())
      buffer.append("}");

    if (AnsiCode.isSupportsColors()) {
      if (level == SEVERE)
        buffer.append("$ANSI{red ");
      else if (level == Level.WARNING)
        buffer.append("$ANSI{yellow ");
      else if (level == Level.INFO)
        buffer.append("$ANSI{green ");
      else if (level == Level.CONFIG)
        buffer.append("$ANSI{green ");
      else if (level == Level.CONFIG)
        buffer.append("$ANSI{white ");
      else
        // DEFAULT
        buffer.append("$ANSI{white ");
    }

    buffer.append(String.format(" %-5.5s ", level.getName()));
    if (AnsiCode.isSupportsColors())
      buffer.append("}");

    if (requester != null) {
      buffer.append("[");
      buffer.append(requester);
      buffer.append("] ");
    }

    // FORMAT THE MESSAGE
    try {
      if (additionalArgs != null)
        buffer.append(String.format(message, additionalArgs));
      else
        buffer.append(message);
    } catch (IllegalFormatException ignore) {
      buffer.append(message);
    }

    return AnsiCode.format(buffer.toString());
  }
}
