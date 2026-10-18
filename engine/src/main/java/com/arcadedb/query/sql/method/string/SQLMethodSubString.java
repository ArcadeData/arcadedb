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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Extracts a sub string from the original.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodSubString extends AbstractSQLMethod {

  public static final String NAME = "substring";

  public SQLMethodSubString() {
    super(NAME, 1, 2);
  }

  @Override
  public String getSyntax() {
    return "subString(<from-index> [,<to-index>])";
  }

  @Override
  public Object execute( final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (value == null || params[0] == null) {
      return null;
    }

    // The indexes count Unicode code points, so a character outside the BMP is never cut in two (issue #10248)
    if (params.length > 1) {
      final int from = Math.max(0, indexArgument(params[0], "from-index"));
      final int to = indexArgument(params[1], "to-index");
      if (to <= from)
        return "";

      return CypherFunctionHelper.substringCodePoints(value.toString(), from, (long) to - from);
    }

    return CypherFunctionHelper.substringCodePoints(value.toString(), Math.max(0, indexArgument(params[0], "from-index")), Long.MAX_VALUE);
  }
}
