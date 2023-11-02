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

import com.arcadedb.log.LogManager;

import java.util.*;
import java.util.logging.*;

/**
 * Utility class for common practices with Java code.
 */
public class CodeUtils {

  public static void executeIgnoringExceptions(final CallableNoReturn callback, final String errorMessage,
      final boolean logException) {
    try {
      callback.call();
    } catch (final Throwable e) {
      // IGNORE IT
      if (logException)
        LogManager.instance().log(CodeUtils.class, Level.SEVERE, errorMessage, e);
      else
        LogManager.instance().log(CodeUtils.class, Level.SEVERE, errorMessage);
    }
  }

  public static void executeIgnoringExceptions(final CallableNoReturn callback) {
    try {
      callback.call();
    } catch (final Throwable e) {
      // IGNORE IT
    }
  }

  /**
   * Compares two objects managing the case objects are NULL.
   */
  public static boolean compare(final Object first, final Object second) {
    if (first == null)
      return second == null;
    return first.equals(second);
  }

  public static List<String> split(final String text, final char sep) {
    return split(text, sep, -1, 10);
  }

  public static List<String> split(final String text, final char sep, final int limit) {
    return split(text, sep, limit, 10);
  }

  public static List<String> split(final String text, final char sep, final int limit, final int estimatedSize) {
    final List<String> parts = limit > -1 ? new ArrayList<>(limit) : new ArrayList<>(estimatedSize);
    int startPos = 0;
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (c == sep) {
        parts.add(text.substring(startPos, i));
        if (limit > -1 && parts.size() >= limit)
          break;
        startPos = i + 1;
      }
    }
    if (startPos < text.length() && (limit == -1 || parts.size() < limit))
      parts.add(text.substring(startPos));
    return parts;
  }

  public static List<String> split(final String text, final String sep) {
    return split(text, sep, -1, 10);
  }

  public static List<String> split(final String text, final String sep, final int limit) {
    return split(text, sep, limit, 10);
  }

  public static List<String> split(final String text, final String sep, final int limit, final int estimatedSize) {
    final List<String> parts = limit > -1 ? new ArrayList<>(limit) : new ArrayList<>(estimatedSize);
    int startPos = 0;
    while (true) {
      final int pos = text.indexOf(sep, startPos);
      if (pos > -1) {
        parts.add(text.substring(startPos, pos));
        if (limit > -1 && parts.size() >= limit)
          break;
        startPos = pos + sep.length();
      } else
        break;
    }

    if (startPos < text.length() && (limit == -1 || parts.size() < limit))
      parts.add(text.substring(startPos));
    return parts;
  }

  public static boolean sleep(final long delay) {
    if (delay > 0)
      try {
        Thread.sleep(delay);
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }
    return false;
  }
}
