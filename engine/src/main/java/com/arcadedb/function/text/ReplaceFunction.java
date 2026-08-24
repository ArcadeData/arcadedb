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
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * replace() function - replaces all occurrences of a substring in a string.
 * Cypher signature: replace(original, search, replace)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplaceFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "replace";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // Every argument is STRING-typed and type-checked before any of them being null decides the answer, so
    // an out-of-domain argument is still reported regardless of which other position happens to be null
    // (issue #5798 review: replace(5, null, 'b') must still be a type error, not a silent null). The search
    // and replacement arguments are STRING-typed too, same as the primary one - issue #6608: #5798 only
    // covered this function's primary argument position, leaving these two to silently coerce via
    // toString().
    final String source = CypherFunctionHelper.requireStringArgument(args[0], getName());
    final String search = CypherFunctionHelper.requireStringArgument(args[1], getName());
    final String replacement = CypherFunctionHelper.requireStringArgument(args[2], getName());
    if (source == null || search == null || replacement == null)
      return null;
    return source.replace(search, replacement);
  }
}
