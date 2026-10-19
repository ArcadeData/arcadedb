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
 * Returns the first characters from the end of the string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodRight extends AbstractSQLMethod {

  public static final String NAME = "right";

  public SQLMethodRight() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "right( <characters>)";
  }

  @Override
  public Object execute( final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null || params[0] == null) {
      return null;
    }

    final String valueAsString = value.toString();

    // A negative offset has no characters to return, same as MySQL's RIGHT(): clamp rather than let
    // substring() throw StringIndexOutOfBoundsException (#5885).
    // Characters are Unicode code points, never half of a surrogate pair (issue #10248).
    final int count = Math.max(0, indexArgument(params[0], "characters"));
    return CypherFunctionHelper.rightCodePoints(valueAsString, count);
  }

}
