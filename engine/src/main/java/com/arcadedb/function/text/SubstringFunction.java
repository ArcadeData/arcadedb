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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * substring() function - returns a substring of the original string.
 */
public class SubstringFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "substring";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // An explicitly written null length propagates rather than meaning "run to the end of the string", which is what
    // CypherSubstringFunction - the implementation Cypher actually resolves substring() to - already did (issue #5193).
    if (args[0] == null || args[1] == null || CypherFunctionHelper.isExplicitNull(args, 2))
      return null;
    final String str = args[0].toString();
    // Issue #6609: a value outside INTEGER | FLOAT (e.g. a LIST) is a client-facing type error, not the unchecked
    // cast's ClassCastException, which used to escape as HTTP 500. Same treatment as the numeric family (#5484).
    final int start = CypherFunctionHelper.requireNumberArgument(args[1], getName()).intValue();
    if (start < 0)
      // Invalid user-supplied argument value: surface as a client error (HTTP 400), matching CypherSubstringFunction
      // (the executor Cypher's substring() actually resolves to) instead of silently returning "". See issue #6609.
      throw new CommandSemanticException("substring(): negative start index is not supported: " + start);
    if (start > str.length())
      return "";
    if (args.length == 3 && args[2] != null) {
      final int length = CypherFunctionHelper.requireNumberArgument(args[2], getName()).intValue();
      if (length < 0)
        // Invalid user-supplied argument value: surface as a client error (HTTP 400), matching CypherSubstringFunction
        // (the executor Cypher's substring() actually resolves to) and left()/right(). See issue #5296/#5793.
        throw new CommandSemanticException("substring(): negative length is not supported: " + length);
      return str.substring(start, Math.min(start + length, str.length()));
    }
    return str.substring(start);
  }
}
