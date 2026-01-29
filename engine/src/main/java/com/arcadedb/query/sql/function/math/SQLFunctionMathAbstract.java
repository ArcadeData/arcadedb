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
package com.arcadedb.query.sql.function.math;

import com.arcadedb.query.sql.function.SQLFunctionConfigurableAbstract;

/**
 * Abstract class for non-aggregate math functions (abs, sqrt, pow, eval).
 * <p>
 * For aggregate math functions (sum, count, avg, min, max, etc.), use
 * {@link com.arcadedb.query.sql.function.SQLAggregatedFunction} instead.
 * </p>
 * <p>
 * This class provides a default {@link #aggregateResults()} implementation
 * that aggregates when called with a single parameter. Non-aggregate functions
 * should override this to return {@code false}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see com.arcadedb.query.sql.function.SQLAggregatedFunction
 */
public abstract class SQLFunctionMathAbstract extends SQLFunctionConfigurableAbstract {

  protected SQLFunctionMathAbstract(final String iName) {
    super(iName);
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }
}
