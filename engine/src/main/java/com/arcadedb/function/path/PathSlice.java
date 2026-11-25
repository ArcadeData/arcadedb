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
 * path.slice(path, offset = 0, length = -1) - Extract a subpath from a path, as APOC's {@code apoc.path.slice}: the
 * {@code length} relationships starting at relationship {@code offset}. A length of -1 reads to the end of the path; a
 * negative offset counts as 0.
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
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Extract the subpath of 'length' relationships starting at relationship 'offset' (length -1 = to the end)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    // A TraversalPath in, a TraversalPath out: the path a MATCH pattern binds used to answer null (issue #9913)
    final TraversalPath path = requirePath(args[0]);
    final int relCount = path.length();
    final long offset = wholeNumber(args.length > 1 ? args[1] : null, 0L, "offset");
    final long length = wholeNumber(args.length > 2 ? args[2] : null, -1L, "length");

    // Issue #12109: the third argument is a LENGTH, as in APOC, not an end index: slice(p, 1, 1) is the second
    // relationship, and -1 (the default) reads to the end. Any other negative length is an empty slice. An empty slice
    // yields a zero-relationship path holding the node it starts at, not null (issue #4707).
    final int from = (int) Math.max(0L, Math.min(offset, relCount));
    final int to;
    if (length == -1L)
      to = relCount;
    else if (length < 0L)
      to = from;
    else
      to = from + (int) Math.min(length, relCount - from);
    return path.subPath(from, to);
  }

  /** A whole number, {@code defaultValue} for null; anything but a number is a type error, not a ClassCastException. */
  private long wholeNumber(final Object value, final long defaultValue, final String argumentName) {
    if (value == null)
      return defaultValue;
    if (value instanceof Number)
      // Issue #10850: a whole number - intValue() read 1.9 as 1 and wrapped 4294967297 to 1. The index stays a long until
      // it is clamped to the path, so one past the int range means "past the end" as it should
      return CypherFunctionHelper.requireWholeNumberArgument(value, getName(), argumentName);
    throw CypherFunctionHelper.typeMismatch(getName(), "an INTEGER", value);
  }
}
