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

import java.util.*;

/**
 * Console ANSI utility class that supports most of the ANSI amenities.
 *
 * @author Luca Garulli
 */
public enum AnsiCode {

  RESET("\u001B[0m"),

  // COLORS
  BLACK("\u001B[30m"), RED("\u001B[31m"), GREEN("\u001B[32m"), YELLOW("\u001B[33m"), BLUE("\u001B[34m"), MAGENTA("\u001B[35m"), CYAN("\u001B[36m"), WHITE(
      "\u001B[37m"),

  HIGH_INTENSITY("\u001B[1m"), LOW_INTENSITY("\u001B[2m"),

  ITALIC("\u001B[3m"), UNDERLINE("\u001B[4m"), BLINK("\u001B[5m"), RAPID_BLINK("\u001B[6m"), REVERSE_VIDEO("\u001B[7m"), INVISIBLE_TEXT("\u001B[8m"),

  BACKGROUND_BLACK("\u001B[40m"), BACKGROUND_RED("\u001B[41m"), BACKGROUND_GREEN("\u001B[42m"), BACKGROUND_YELLOW("\u001B[43m"), BACKGROUND_BLUE(
      "\u001B[44m"), BACKGROUND_MAGENTA("\u001B[45m"), BACKGROUND_CYAN("\u001B[46m"), BACKGROUND_WHITE("\u001B[47m"),

  NULL("");

  private final static boolean SUPPORTS_COLORS;
  private final        String  code;

  AnsiCode(final String code) {
    this.code = code;
  }

  @Override
  public String toString() {
    return code;
  }

  public static boolean supportsColors() {
    return SUPPORTS_COLORS;
  }

  static {
    final String ansiSupport = "auto";
    if ("true".equalsIgnoreCase(ansiSupport))
      // FORCE ANSI SUPPORT
      SUPPORTS_COLORS = true;
    else if ("auto".equalsIgnoreCase(ansiSupport)) {
      // AUTOMATIC CHECK
      SUPPORTS_COLORS = System.console() != null && !System.getProperty("os.name").toLowerCase().contains("win");
    } else
      // DO NOT SUPPORT ANSI
      SUPPORTS_COLORS = false;
  }

  public static String format(final String message) {
    return format(message, SUPPORTS_COLORS);
  }

  public static String format(final String message, final boolean supportsColors) {
    return (String) VariableParser.resolveVariables(message, "$ANSI{", "}", iVariable -> {
      final int pos = iVariable.indexOf(' ');

      final String text = pos > -1 ? iVariable.substring(pos + 1) : "";

      if (supportsColors) {
        final String code = pos > -1 ? iVariable.substring(0, pos) : iVariable;

        final StringBuilder buffer = new StringBuilder();

        final List<String> codes = CodeUtils.split(code, ':');
        for (int i = 0; i < codes.size(); ++i)
          buffer.append(AnsiCode.valueOf(codes.get(i).toUpperCase(Locale.ENGLISH)));

        if (pos > -1) {
          buffer.append(text);
          buffer.append(AnsiCode.RESET);
        }

        return buffer.toString();
      }

      return text;
    });
  }
}
