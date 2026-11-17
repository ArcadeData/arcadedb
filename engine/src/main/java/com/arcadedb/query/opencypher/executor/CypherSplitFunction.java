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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Cypher split() function - splits a string by a delimiter, or by any of a list of delimiters.
 * Returns null if either string or delimiter is null (Cypher behavior).
 */
public class CypherSplitFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "split";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // Both arguments are STRING-typed and type-checked before either being null decides the answer, so an
    // out-of-domain argument is still reported regardless of which position happens to be null (issue #5798
    // review: split(5, null) must still be a type error, not a silent null). The delimiter is STRING-typed
    // too, same as the primary argument - issue #6608: #5798 only covered this function's primary argument
    // position, leaving the delimiter to silently coerce via toString(). Neo4j itself coerces a non-STRING
    // delimiter, but ArcadeDB's own #5798 policy already rejects it in the primary position of this same
    // function, so accepting it here would be an inconsistency within this build rather than a Neo4j
    // compatibility concern.
    final String str = CypherFunctionHelper.requireStringArgument(args[0], getName());

    final Object delimiter = args[1];
    if (delimiter == null)
      return null;

    if (delimiter instanceof String text) {
      if (str == null)
        return null;

      // The one split of the engine, shared with the SQL split() method and function and with text.split() (issue #10485): an empty
      // delimiter splits into characters (Neo4j/Memgraph semantics), any other is a literal found with indexOf rather than a regex
      // compiled on every call, the empty pieces are kept, and a surrogate pair is never cut.
      return CypherFunctionHelper.splitLiteral(str, text);
    }

    // Neo4j 5 also accepts split(original :: STRING, splitDelimiters :: LIST<STRING>), which splits the text on any
    // of the delimiters of the list (issue #12396).
    final List<Object> list = CypherFunctionHelper.asListOrNull(delimiter);
    if (list != null) {
      final List<String> delimiters = new ArrayList<>(list.size());
      for (final Object element : list) {
        // a null delimiter makes the whole answer null, as in Neo4j
        if (element == null)
          return null;
        if (!(element instanceof String text))
          throw CypherFunctionHelper.typeMismatch(getName(), "a STRING or a LIST<STRING>", element);
        delimiters.add(text);
      }
      if (str == null)
        return null;

      return CypherFunctionHelper.splitLiteral(str, delimiters);
    }

    throw CypherFunctionHelper.typeMismatch(getName(), "a STRING or a LIST<STRING>", delimiter);
  }
}
