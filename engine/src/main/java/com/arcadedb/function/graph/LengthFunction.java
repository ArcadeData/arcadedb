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
package com.arcadedb.function.graph;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * length() function - returns the length of a path (number of relationships). The path is read in any shape
 * {@link TraversalPath#of(Object)} reads; a string answers its length, as it always has; anything else is a type error
 * rather than 0 (issue #9913).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class LengthFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "length";
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
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);

    if (args[0] == null)
      return null;
    final TraversalPath path = TraversalPath.of(args[0]);
    if (path != null)
      return (long) path.length();
    // Also support string length for compatibility
    if (args[0] instanceof String string)
      return (long) string.length();
    throw CypherFunctionHelper.typeMismatch(getName(), "a PATH or a STRING", args[0]);
  }
}
