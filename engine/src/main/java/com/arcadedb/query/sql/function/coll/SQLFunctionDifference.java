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

import java.util.*;

/**
 * This operator can work inline. Returns the DIFFERENCE between the collections received as parameters. Works also with no
 * collection values.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionDifference extends SQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "difference";

  public SQLFunctionDifference() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null)
      return null;

    // IN-LINE MODE (STATELESS)
    final Set<Object> result = new HashSet<Object>();

    boolean first = true;
    for (final Object iParameter : params) {
      if (first) {
        if (iParameter instanceof Collection<?>) {
          result.addAll((Collection<Object>) iParameter);
        } else {
          result.add(iParameter);
        }
      } else {
        if (iParameter instanceof Collection<?>) {
          result.removeAll((Collection<Object>) iParameter);
        } else {
          result.remove(iParameter);
        }
      }

      first = false;
    }

    return result;

  }

  public String getSyntax() {
    return "difference(<field>, <field> [, <field]*)";
  }
}
