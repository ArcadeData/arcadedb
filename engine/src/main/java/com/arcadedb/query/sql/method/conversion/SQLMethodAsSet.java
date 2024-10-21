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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * Transforms current value in a Set.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsSet extends AbstractSQLMethod {

  public static final String NAME = "asset";

  public SQLMethodAsSet() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(Object value, final Identifiable iCurrentRecord, final CommandContext iContext, final Object[] iParams) {
    if (value instanceof Set)
      // ALREADY A SET
      return value;

    if (value == null)
      // NULL VALUE, RETURN AN EMPTY SET
      return Collections.emptySet();

    if (value instanceof Collection<?>) {
      return new HashSet<>((Collection<Object>) value);
    } else if (value instanceof Iterable<?>) {
      value = ((Iterable<?>) value).iterator();
    }

    if (value instanceof Iterator<?>) {
      final Set<Object> set = new HashSet<>();

      for (final Iterator<Object> iter = (Iterator<Object>) value; iter.hasNext(); )
        set.add(iter.next());

      return set;
    }

    // SINGLE ITEM: ADD IT AS UNIQUE ITEM
    return Collections.singleton(value);
  }
}
