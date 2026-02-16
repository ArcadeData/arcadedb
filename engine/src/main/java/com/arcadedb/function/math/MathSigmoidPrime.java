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
 * math.sigmoidPrime(x) - Derivative of sigmoid: sigmoid(x) * (1 - sigmoid(x))
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MathSigmoidPrime extends AbstractMathFunction {
  @Override
  protected String getSimpleName() {
    return "sigmoidPrime";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Calculate the derivative of sigmoid: sigmoid(x) * (1 - sigmoid(x))";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final double x = asDouble(args[0]);
    final double sigmoid = 1.0 / (1.0 + Math.exp(-x));
    return sigmoid * (1.0 - sigmoid);
  }
}
