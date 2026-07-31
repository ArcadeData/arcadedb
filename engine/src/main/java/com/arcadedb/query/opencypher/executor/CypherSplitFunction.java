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
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Cypher split() function - splits a string by a delimiter.
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
    if (args[0] == null || args[1] == null)
      return null;
    final String str = args[0].toString();
    final String delimiter = args[1].toString();

    // An empty delimiter splits the string into its individual characters (Neo4j/Memgraph semantics).
    // Java's String.split("", -1) appends a spurious trailing empty string, so handle this case explicitly.
    if (delimiter.isEmpty()) {
      if (str.isEmpty())
        return List.of("");
      final List<String> characters = new ArrayList<>(str.length());
      str.codePoints().forEach(cp -> characters.add(new String(Character.toChars(cp))));
      return characters;
    }

    return List.of(str.split(Pattern.quote(delimiter), -1));
  }
}
