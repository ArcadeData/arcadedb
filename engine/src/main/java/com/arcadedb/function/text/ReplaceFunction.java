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
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 3)
      throw new CommandExecutionException("replace() requires exactly 3 arguments: replace(original, search, replace)");
    if (args[0] == null)
      return null;
    return args[0].toString().replace(args[1].toString(), args[2].toString());
  }
}
