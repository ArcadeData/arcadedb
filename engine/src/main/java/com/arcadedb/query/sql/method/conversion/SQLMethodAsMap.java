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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * Transforms current value into a Map.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsMap extends AbstractSQLMethod {

  public static final String NAME = "asmap";

  public SQLMethodAsMap() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object value, final Identifiable iCurrentRecord, final CommandContext iContext,
      final Object[] iParams) {
    if (value instanceof Map)
      // ALREADY A MAP
      return value;
    else if (value == null)
      // NULL VALUE, RETURN AN EMPTY MAP
      return Collections.emptyMap();
    else if (value instanceof Document)
      // CONVERT DOCUMENT TO MAP
      return ((Document) value).toMap(false);

    final Iterator<Object> iter;
    if (value instanceof Iterator<?>)
      iter = (Iterator<Object>) value;
    else if (value instanceof Iterable<?>)
      iter = ((Iterable<Object>) value).iterator();
    else
      return null;

    final HashMap<String, Object> map = new HashMap<>();
    while (iter.hasNext()) {
      final Object key = iter.next();
      if (iter.hasNext())
        map.put((String) key, iter.next());
    }

    return map;
  }
}
