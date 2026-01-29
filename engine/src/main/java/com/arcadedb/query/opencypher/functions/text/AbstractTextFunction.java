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
package com.arcadedb.query.opencypher.functions.text;

import com.arcadedb.query.opencypher.functions.CypherFunction;

/**
 * Abstract base class for text functions.
 * All text functions share the "text." namespace prefix.
 *
 * @author ArcadeDB Team
 */
public abstract class AbstractTextFunction implements CypherFunction {
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
   * Safely converts an argument to int, with default value for null.
   */
  protected int asInt(final Object arg, final int defaultValue) {
    if (arg == null) {
      return defaultValue;
    }
    if (arg instanceof Number) {
      return ((Number) arg).intValue();
    }
    return Integer.parseInt(arg.toString());
  }
}
