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

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * math.round(value, precision, mode) - APOC-namespaced entry point for the standard round() function,
 * so it is also reachable as apoc.math.round(). See {@link RoundFunction} for the rounding semantics.
 */
public class MathRound extends AbstractMathFunction {
  private final RoundFunction delegate = new RoundFunction();

  @Override
  protected String getSimpleName() {
    return "round";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Rounds a number to the nearest integer or to a specified number of decimal places";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // Checked here, not left to the delegate: RoundFunction.checkArity would use its own getName() ("round"),
    // naming the wrong function in the error message for a call made as math.round()/apoc.math.round().
    checkArity(args);
    return delegate.execute(args, context);
  }
}
