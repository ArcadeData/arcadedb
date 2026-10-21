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

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.split(string, delimiter) - Split string into list.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSplit extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
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
  public String getDescription() {
    return "Split string into a list using the specified delimiter";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    // A missing or empty delimiter splits into individual characters. The one split of the engine (issue #10485), shared with
    // the SQL method and the openCypher function: a pair is a character, never cut, and the empty pieces are kept
    final String delimiter = asString(args[1]);
    return CypherFunctionHelper.splitLiteral(str, delimiter == null ? "" : delimiter);
  }
}
