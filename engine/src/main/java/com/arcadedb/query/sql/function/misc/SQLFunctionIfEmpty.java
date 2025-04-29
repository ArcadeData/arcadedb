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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.*;

/**
 * Returns the passed {@literal field/value} (or optional parameter {@literal return_value_if_not_empty}) if
 * {@literal field/value} is <b>not</b> empty; otherwise it returns {@literal return_value_if_empty}.
 *
 * <br>
 * Syntax: <blockquote>
 *
 * <pre>
 * ifempty(&lt;field|value&gt;, &lt;return_value_if_empty&gt; [,&lt;return_value_if_not_empty&gt;])
 * </pre>
 *
 * </blockquote>
 *
 * <br>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>ifempty('a', 'b')</b> FROM ...
 *  -&gt; 'a'
 *
 * SELECT <b>ifempty('a', 'b', 'c')</b> FROM ...
 *  -&gt; 'c'
 *
 * SELECT <b>ifempty('', 'b')</b> FROM ...
 *  -&gt; 'b'
 *
 * SELECT <b>ifempty([], 'b', 'c')</b> FROM ...
 *  -&gt; 'b'
 * </pre>
 *
 * </blockquote>
 *
 * @author gramian
 */

public class SQLFunctionIfEmpty extends SQLFunctionAbstract {

  public static final String NAME = "ifempty";

  public SQLFunctionIfEmpty() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    /*
     * iFuncParams [0] field/value to check for empty [1] return value if [0] is empty [2] optional return value if [0] is not empty
     */
    if ( (params[0] instanceof String && params[0].toString().length() == 0)
         || (params[0] instanceof Collection<?> && MultiValue.getSize(params[0]) == 0) ) {
      return params[1];
    }
      if (params.length == 3) {
        return params[2];
      }
      return params[0];
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifempty(<field|value>, <return_value_if_empty> [,<return_value_if_not_empty>])";
  }
}
