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

import java.util.logging.*;

/**
 * Utility class for common practices with Java code.
 */
public class CodeUtils {

  public static void executeIgnoringExceptions(final CallableNoReturn callback, final String errorMessage, final boolean logException) {
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
}
