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

import com.arcadedb.exception.CommandExecutionException;
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
    final int start = ((Number) args[1]).intValue();
    if (start < 0 || start > str.length())
      return "";
    if (args.length == 3 && args[2] != null) {
      final int length = ((Number) args[2]).intValue();
      if (length < 0)
        throw new CommandExecutionException("substring(): negative length is not supported: " + length);
      return str.substring(start, Math.min(start + length, str.length()));
    }
    return str.substring(start);
  }
}
