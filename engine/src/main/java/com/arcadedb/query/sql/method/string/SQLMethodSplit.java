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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Splits a string using a delimiter.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodSplit extends AbstractSQLMethod {

  public static final String NAME = "split";

  public SQLMethodSplit() {
    super(NAME, 1);
  }

  @Override
  public Object execute(final Object value, final Identifiable iRecord, final CommandContext context, final Object[] params) {
    if (value == null || null == params || null == params[0])
      return value;

    // The delimiter is a literal string, not a regex (issue #5886: matches split()/text.split()/Cypher split(),
    // which all wrap the delimiter in Pattern.quote() for the same reason - this method was the only one of the
    // four that passed the caller-supplied delimiter straight to String.split(regex) unescaped, exposing it to
    // the same catastrophic-backtracking risk as every other regex entry point in that issue).
    // A List, not the String[] String.split() hands back (issue #7027): the SQL split() function, the Cypher split()
    // function and the documentation of this very method all answer a list, and this was the only one of the four
    // that leaked the raw Java array - the one receiver shape most of the collection methods then mishandled.
    return List.of(value.toString().split(Pattern.quote(params[0].toString())));
  }
}
