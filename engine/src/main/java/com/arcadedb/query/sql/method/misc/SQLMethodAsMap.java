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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
  public Object execute(final Object iThis, Identifiable iCurrentRecord, CommandContext iContext, Object ioResult, Object[] iParams) {
    if (ioResult instanceof Map)
      // ALREADY A MAP
      return ioResult;

    if (ioResult == null) {
      // NULL VALUE, RETURN AN EMPTY MAP
      return Collections.EMPTY_MAP;
    }

    if (ioResult instanceof Document) {
      // CONVERT DOCUMENT TO MAP
      return ((Document) ioResult).toMap();
    }

    Iterator<Object> iter;
    if (ioResult instanceof Iterator<?>) {
      iter = (Iterator<Object>) ioResult;
    } else if (ioResult instanceof Iterable<?>) {
      iter = ((Iterable<Object>) ioResult).iterator();
    } else {
      return null;
    }

    final HashMap<String, Object> map = new HashMap<String, Object>();
    while (iter.hasNext()) {
      final Object key = iter.next();
      if (iter.hasNext()) {
        final Object value = iter.next();
        map.put((String) key, value);
      }
    }

    return map;
  }
}
