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

import java.util.HashMap;
import java.util.Map;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionMap extends SQLFunctionMultiValueAbstract<Map<String, Object>> {
  public static final String NAME = "map";

  public SQLFunctionMap() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams, CommandContext iContext) {

    if (iParams.length > 2)
      // IN LINE MODE
      context = new HashMap<>();

    if (iParams.length == 1) {
      if (iParams[0] == null)
        return null;

      if (iParams[0] instanceof Map<?, ?>) {
        if (context == null)
          // AGGREGATION MODE (STATEFUL)
          context = new HashMap<>();

        // INSERT EVERY SINGLE COLLECTION ITEM
        context.putAll((Map<String, Object>) iParams[0]);
      } else
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else if (iParams.length % 2 != 0)
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    else
      for (int i = 0; i < iParams.length; i += 2) {
        final String key = (String) iParams[i];
        final Object value = iParams[i + 1];

        if (value != null) {
          if (iParams.length <= 2 && context == null)
            // AGGREGATION MODE (STATEFUL)
            context = new HashMap<>();

          context.put(key, value);
        }
      }

    return prepareResult(context);
  }

  public String getSyntax() {
    return "map(<map>|[<key>,<value>]*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return configuredParameters.length <= 2;
  }

  @Override
  public Map<String, Object> getResult() {
    final Map<String, Object> res = context;
    context = null;
    return prepareResult(res);
  }

  protected Map<String, Object> prepareResult(final Map<String, Object> res) {
    return res;
  }
}
