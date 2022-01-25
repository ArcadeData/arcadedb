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
 * Returns the first {@literal field/value} not null parameter. if no {@literal field/value} is <b>not</b> null, returns null.
 * <br>
 * <br>
 * Syntax: <blockquote>
 *
 * <pre>
 * coalesce(&lt;field|value&gt;[,&lt;field|value&gt;]*)
 * </pre>
 *
 * </blockquote>
 * <br>
 * <br>
 * Examples: <blockquote>
 *
 * <pre>
 * SELECT <b>coalesce('a', 'b')</b> FROM ...
 *  -&gt; 'a'
 *
 * SELECT <b>coalesce(null, 'b')</b> FROM ...
 *  -&gt; 'b'
 *
 * SELECT <b>coalesce(null, null, 'c')</b> FROM ...
 *  -&gt; 'c'
 *
 * SELECT <b>coalesce(null, null)</b> FROM ...
 *  -&gt; null
 *
 * </pre>
 *
 * </blockquote>
 *
 * @author Claudio Tesoriero
 */

public class SQLFunctionCoalesce extends SQLFunctionAbstract {
  public static final String NAME = "coalesce";

  public SQLFunctionCoalesce() {
    super(NAME);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      CommandContext iContext) {
    int length = iParams.length;
    for (int i = 0; i < length; i++) {
      if (iParams[i] != null)
        return iParams[i];
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "Returns the first not-null parameter or null if all parameters are null. Syntax: coalesce(<field|value> [,<field|value>]*)";
  }
}
