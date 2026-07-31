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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.function.DoubleBinaryOperator;

/**
 * Generic math binary function (e.g. atan2). Always returns a Double, matching the
 * FLOAT return type declared by the Cypher signature of these functions (issue #5382).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MathBinaryFunction implements StatelessFunction {
  private final String name;
  private final DoubleBinaryOperator op;

  public MathBinaryFunction(final String name, final DoubleBinaryOperator op) {
    this.name = name;
    this.op = op;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // Both arguments are type-checked before null propagation decides the answer, so an out-of-domain argument is
    // still reported when the other one happens to be null (issue #5484).
    final Number first = CypherFunctionHelper.requireNumberArgument(args[0], name);
    final Number second = CypherFunctionHelper.requireNumberArgument(args[1], name);
    if (first == null || second == null)
      return null;
    return op.applyAsDouble(first.doubleValue(), second.doubleValue());
  }
}
