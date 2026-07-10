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
package com.arcadedb.function.math;

import com.arcadedb.function.StatelessFunction;

/**
 * Abstract base class for math functions.
 * All math functions share the "math." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractMathFunction implements StatelessFunction {
  protected static final String NAMESPACE = "math";

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Extract a double from a numeric argument, propagating null.
   * <p>
   * Returns {@code null} when the argument is null so the caller can short-circuit and return
   * null per the Cypher spec (and matching PostgreSQL/MySQL, where math functions are null-in
   * null-out). Returning a boxed {@link Double} avoids the previous foot-gun where a null was
   * silently coerced to {@code 0.0}, turning e.g. {@code tanh(null)} into {@code tanh(0) == 0}.
   * See issue #4556.
   */
  protected Double asDouble(final Object arg) {
    if (arg == null)
      return null;
    if (arg instanceof Number number) {
      return number.doubleValue();
    }
    return Double.parseDouble(arg.toString());
  }
}
