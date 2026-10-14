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
package com.arcadedb.function.path;

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * path.slice(path, from, [to]) - Extract a subpath from a path.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSlice extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "slice";
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
  public String getDescription() {
    return "Extract a subpath from index 'from' to 'to' (based on relationship count)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    // A TraversalPath in, a TraversalPath out: the path a MATCH pattern binds used to answer null (issue #9913)
    final TraversalPath path = requirePath(args[0]);
    final int relCount = path.length();
    final int from = index(args[1], 0);
    final int to = index(args.length > 2 ? args[2] : null, relCount);

    // Clamp the bounds. An empty slice (from == to, or from == relCount, i.e. slicing at the path's
    // end) yields a zero-relationship path containing just the boundary node, not null (issue #4707).
    final int actualFrom = Math.max(0, Math.min(from, relCount));
    final int actualTo = Math.max(actualFrom, Math.min(relCount, to));
    return path.subPath(actualFrom, actualTo);
  }

  /** A relationship index, {@code defaultValue} for null; anything but a number is a type error, not a ClassCastException. */
  private int index(final Object value, final int defaultValue) {
    if (value == null)
      return defaultValue;
    if (value instanceof Number number)
      return number.intValue();
    throw CypherFunctionHelper.typeMismatch(getName(), "an INTEGER", value);
  }
}
