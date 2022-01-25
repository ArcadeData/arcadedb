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
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

/**
 * Returns the passed {@literal field/value} (or optional parameter {@literal return_value_if_not_null}) if
 * {@literal field/value} is <b>not</b> null; otherwise it returns {@literal return_value_if_null}.
 *
 * <br>
 * Syntax: <blockquote>
 *
 * <pre>
 * ifnull(&lt;field|value&gt;, &lt;return_value_if_null&gt; [,&lt;return_value_if_not_null&gt;])
 * </pre>
 *
 * </blockquote>
 *
 * <br>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>ifnull('a', 'b')</b> FROM ...
 *  -&gt; 'a'
 *
 * SELECT <b>ifnull('a', 'b', 'c')</b> FROM ...
 *  -&gt; 'c'
 *
 * SELECT <b>ifnull(null, 'b')</b> FROM ...
 *  -&gt; 'b'
 *
 * SELECT <b>ifnull(null, 'b', 'c')</b> FROM ...
 *  -&gt; 'b'
 * </pre>
 *
 * </blockquote>
 *
 * @author Mark Bigler
 */

public class SQLFunctionIfNull extends SQLFunctionAbstract {

  public static final String NAME = "ifnull";

  public SQLFunctionIfNull() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {
    /*
     * iFuncParams [0] field/value to check for null [1] return value if [0] is null [2] optional return value if [0] is not null
     */
    if (iParams[0] != null) {
      if (iParams.length == 3) {
        return iParams[2];
      }
      return iParams[0];
    }
    return iParams[1];
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifnull(<field|value>, <return_value_if_null> [,<return_value_if_not_null>])";
  }
}
