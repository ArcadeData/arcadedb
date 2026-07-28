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
package com.arcadedb.function.coll;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;
import java.util.Map;

/**
 * size() function - returns the number of characters of a STRING, or the number of entries of a LIST or a MAP.
 * <p>
 * Anything else is a type error, not a null result (issue #5477); {@code null} propagates to {@code null}. Maps are counted
 * rather than rejected, following Memgraph: Neo4j has no size() for maps at all, so nothing is lost by accepting them.
 */
public class SizeFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "size";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("size() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof CharSequence text)
      return (long) text.length();
    if (args[0] instanceof Map<?, ?> map)
      return (long) map.size();
    // Accept List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284).
    final List<Object> list = CypherFunctionHelper.asListOrNull(args[0]);
    if (list != null)
      return (long) list.size();
    // A number, a boolean, a node, a relationship or a path has no size: answering null here would be indistinguishable
    // from null propagation, so a wrong query would look like a successful one (issue #5477).
    throw CypherFunctionHelper.typeMismatch("size", "a STRING, a LIST<ANY> or a MAP", args[0]);
  }
}
