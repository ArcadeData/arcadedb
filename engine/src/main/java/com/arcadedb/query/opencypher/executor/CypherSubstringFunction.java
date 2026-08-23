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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Cypher substring() function - returns a substring of the original string.
 * Cypher uses 0-based indexing and raises error for negative start/length.
 */
public class CypherSubstringFunction implements StatelessFunction {
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
    if (args[0] == null || args[1] == null)
      return null;
    final String str = args[0].toString();
    // Issue #6609: a value outside INTEGER | FLOAT (e.g. a LIST) is a client-facing type error, not the unchecked
    // cast's ClassCastException, which used to escape as HTTP 500. Same treatment as the numeric family (#5484).
    final int start = CypherFunctionHelper.requireNumberArgument(args[1], getName()).intValue();
    if (start < 0)
      // Invalid user-supplied argument value: surface as a client error (HTTP 400), matching left()/right().
      // CommandSemanticException extends CommandParsingException, which the HTTP handler maps to 400. See issue #5793/#5296.
      throw new CommandSemanticException("substring(): negative start index is not supported: " + start);
    // Issue #5193/#5809: an explicitly supplied null length propagates null (as Neo4j does), it must not be
    // treated as an omitted argument. This check must run before the start-past-the-end short-circuit below,
    // otherwise null propagation silently stops once start reaches the length of the string.
    if (CypherFunctionHelper.isExplicitNull(args, 2))
      return null;
    if (start >= str.length())
      return "";
    if (args.length == 3) {
      final int length = CypherFunctionHelper.requireNumberArgument(args[2], getName()).intValue();
      if (length < 0)
        throw new CommandSemanticException("substring(): negative length is not supported: " + length);
      return str.substring(start, Math.min(start + length, str.length()));
    }
    return str.substring(start);
  }
}
