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
package com.arcadedb.function.text;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;

/**
 * Abstract base class for text functions.
 * All text functions share the "text." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractTextFunction implements StatelessFunction {
  protected static final String NAMESPACE = "text";

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Safely converts an argument to String, handling null values.
   */
  protected String asString(final Object arg) {
    return arg == null ? null : arg.toString();
  }

  /**
   * The characters of a string, one {@code int} each: a character outside the BMP is one element, not the two UTF-16 units of its
   * surrogate pair. A lone surrogate is a character of its own.
   */
  protected static int[] codePointsOf(final String s) {
    final int length = s.length();
    final int[] codePoints = new int[s.codePointCount(0, length)];
    for (int i = 0, n = 0; i < length; n++) {
      final int codePoint = s.codePointAt(i);
      codePoints[n] = codePoint;
      i += Character.charCount(codePoint);
    }
    return codePoints;
  }

  /**
   * Reads an int argument, with a default value for null. A number has to be a whole one in the int range: intValue()
   * read 2.9 as 2 and wrapped 4294967298 to 2, so a wrong argument answered as a plausible one (issue #10850). A text is
   * parsed as a whole number too, and one that is not is the same client error (issue #10954).
   */
  protected int asInt(final Object arg, final int defaultValue, final String argumentName) {
    if (arg == null)
      return defaultValue;
    return CypherFunctionHelper.requireWholeIntOrTextArgument(arg, getName(), argumentName);
  }
}
