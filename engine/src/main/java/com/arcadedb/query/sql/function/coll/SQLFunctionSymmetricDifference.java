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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the SYMMETRIC DIFFERENCE between the collections received as parameters. Works also with no collection values.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionSymmetricDifference extends SQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "symmetricDifference";

  private Set<Object> rejected;

  public SQLFunctionSymmetricDifference() {
    super(NAME, 1, -1);
  }

  private static void addItemToResult(Object o, Set<Object> accepted, Set<Object> rejected) {
    if (!accepted.contains(o) && !rejected.contains(o)) {
      accepted.add(o);
    } else {
      accepted.remove(o);
      rejected.add(o);
    }
  }

  private static void addItemsToResult(Collection<Object> co, Set<Object> accepted, Set<Object> rejected) {
    for (Object o : co) {
      addItemToResult(o, accepted, rejected);
    }
  }

  @SuppressWarnings("unchecked")
  public Object execute(Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams, CommandContext iContext) {
    if (iParams[0] == null)
      return null;

    Object value = iParams[0];

    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      if (context == null) {
        context = new HashSet<Object>();
        rejected = new HashSet<Object>();
      }
      if (value instanceof Collection<?>) {
        addItemsToResult((Collection<Object>) value, context, rejected);
      } else {
        addItemToResult(value, context, rejected);
      }

      return null;
    } else {
      // IN-LINE MODE (STATELESS)
      final Set<Object> result = new HashSet<Object>();
      final Set<Object> rejected = new HashSet<Object>();

      for (Object iParameter : iParams) {
        if (iParameter instanceof Collection<?>) {
          addItemsToResult((Collection<Object>) iParameter, result, rejected);
        } else {
          addItemToResult(iParameter, result, rejected);
        }
      }

      return result;
    }
  }

  public String getSyntax() {
    return "difference(<field>*)";
  }
}
