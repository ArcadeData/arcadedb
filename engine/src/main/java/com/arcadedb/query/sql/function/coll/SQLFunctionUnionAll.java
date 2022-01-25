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
package com.arcadedb.query.sql.function.coll;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.utility.MultiIterator;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns, a
 * UNION of the collections received as parameters. Works also with no collection values. Does not remove duplication from the
 * result.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionUnionAll extends SQLFunctionMultiValueAbstract<Collection<Object>> {
  public static final String NAME = "unionAll";

  public SQLFunctionUnionAll() {
    super(NAME, 1, -1);
  }

  public Object execute( final Object iThis, final Identifiable iCurrentRecord,
      final Object iCurrentResult, final Object[] iParams, CommandContext iContext) {
    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      Object value = iParams[0];
      if (value != null) {

        if (context == null)
          context = new ArrayList<Object>();

        MultiValue.add(context, value);
      }

      return context;
    } else {
      // IN-LINE MODE (STATELESS)
      final MultiIterator<Identifiable> result = new MultiIterator<>();
      for (Object value : iParams) {
        if (value != null)
          result.addIterator(value);
      }

      return result;
    }
  }

  public String getSyntax() {
    return "unionAll(<field>*)";
  }
}
