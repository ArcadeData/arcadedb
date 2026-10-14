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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collection;

/**
 * path.combine(path1, path2) - Joins two paths into one, the second continuing from where the first ends (APOC's
 * {@code apoc.path.combine}).
 * <p>
 * The end node of {@code path1} must be the start node of {@code path2}; a {@code null} path leaves the other one as
 * the result. Every path is read in any shape {@link TraversalPath#of(Object)} reads - the path a {@code MATCH} binds, the
 * one a path procedure yields, the map a client sends back - and the result is a
 * {@link TraversalPath}, which {@code nodes()}, {@code relationships()}, {@code length()} and the drivers read as one
 * (issues #9913, #10017). The single-argument form {@code path.combine([p1, p2, ...])} joins a list of paths in order.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathCombine extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "combine";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Combine two paths, the second starting where the first ends, into a single path";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args.length == 2)
      return combine(args[0], args[1]);

    final Object arg = args[0];
    if (arg == null)
      return null;

    final TraversalPath single = TraversalPath.of(arg);
    if (single != null)
      return single;

    if (!(arg instanceof Collection<?> paths))
      throw notAPath(arg);

    // A LIST OF PATHS, JOINED IN ORDER
    TraversalPath combined = null;
    for (final Object path : paths)
      combined = combine(combined, path);
    return combined;
  }

  private static TraversalPath combine(final Object first, final Object second) {
    if (first == null)
      return second == null ? null : asPath(second);
    if (second == null)
      return asPath(first);

    final TraversalPath firstPath = asPath(first);
    final TraversalPath secondPath = asPath(second);
    if (!firstPath.getEndVertex().getIdentity().equals(secondPath.getStartVertex().getIdentity()))
      throw new CommandExecutionException("path.combine(): the paths do not connect, the first ends at "
          + firstPath.getEndVertex().getIdentity() + " and the second starts at " + secondPath.getStartVertex().getIdentity());
    return new TraversalPath(firstPath, secondPath);
  }

  /** The path {@code value} is, refusing anything else so a value that is not a path never comes back as a "combined path". */
  private static TraversalPath asPath(final Object value) {
    final TraversalPath path = TraversalPath.of(value);
    if (path == null)
      throw notAPath(value);
    return path;
  }

  private static CommandExecutionException notAPath(final Object value) {
    return new CommandExecutionException(
        "path.combine() expects paths, got " + (value == null ? "null" : value.getClass().getSimpleName()));
  }
}
