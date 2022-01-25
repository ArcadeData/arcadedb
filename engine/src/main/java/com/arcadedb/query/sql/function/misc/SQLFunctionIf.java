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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.logging.Level;

/**
 * Returns different values based on the condition. If it's true the first value is returned, otherwise the second one.
 * <br>
 * Syntax: <blockquote>
 *
 * {@literal if(&lt;field|value|expression&gt;, &lt;return_value_if_true&gt; [,&lt;return_value_if_false&gt;])}
 *
 * </blockquote>
 * <br>
 * <br>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>if(rich, 'rich', 'poor')</b> FROM ...
 * <br>
 * SELECT <b>if( eval( 'salary &gt; 1000000' ), 'rich', 'poor')</b> FROM ...
 * </pre>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */

public class SQLFunctionIf extends SQLFunctionAbstract {

  public static final String NAME = "if";

  public SQLFunctionIf() {
    super(NAME);
  }

  @Override
  public Object execute(Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {

    boolean result;

    try {
      Object condition = iParams[0];
      if (condition instanceof Boolean)
        result = (Boolean) condition;
      else if (condition instanceof String)
        result = Boolean.parseBoolean(condition.toString());
      else if (condition instanceof Number)
        result = ((Number) condition).intValue() > 0;
      else
        return null;

      return result ? iParams[1] : iParams[2];

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error during if execution", e);

      return null;
    }
  }

  @Override
  public String getSyntax() {
    return "if(<field|value|expression>, <return_value_if_true> [,<return_value_if_false>])";
  }
}
