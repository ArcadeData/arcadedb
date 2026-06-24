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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Remove the first occurrence of elements from a collection.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see SQLMethodRemoveAll
 */
public class SQLMethodRemove extends AbstractSQLMethod {

  public static final String NAME = "remove";

  public SQLMethodRemove() {
    super(NAME, 1, -1);
  }

  @Override
  public Object execute(Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (params != null && params.length > 0 && params[0] != null) {
      final Object[] arguments = MultiValue.array(params, Object.class, argument -> {
        if (argument instanceof String string && string.startsWith("$")) {
          return context.getVariable(string);
        }
        return argument;
      });
      // Work on a copy so the source value (e.g. a record property) is not mutated in place. Without it,
      // `UPDATE ... SET x = x.remove(...)` would return the same already-mutated instance as the current value,
      // making UPDATE skip the write and leave the change unpersisted (issue #4730).
      value = MultiValue.copy(value);
      for (final Object o : arguments) {
        value = MultiValue.remove(value, o, false);
      }
      return value;
    }

    return value;
  }

  @Override
  public String getSyntax() {
    return "remove(<item>*)";
  }
}
