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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.serializer.BinaryComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Christian Himpe
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodSort extends AbstractSQLMethod {

  public static final String NAME = "sort";

  public SQLMethodSort() {
    super(NAME, 0, 1);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {

    // ANY COLLECTION, NOT ONLY A List: A SET OR AN ARRAY RECEIVER USED TO COME BACK UNSORTED WITH NO ERROR, WHICH IS
    // WORSE THAN A FAILURE BECAUSE IT LOOKS LIKE IT WORKED (ISSUE #7027). SCALARS STAY AN IDENTITY.
    final List<Object> list = listReceiverOrNull(value);
    if (list != null) {
      final List<Object> result = new ArrayList<>(list);
      final boolean descending = params != null && params.length > 0 && params[0] instanceof Boolean bool && !bool;
      if (holdsRows(result))
        sortRows(result, descending);
      else if (descending)
        result.sort((left, right) -> BinaryComparator.compareTo(right, left));
      else
        result.sort((left, right) -> BinaryComparator.compareTo(left, right));
      return result;
    } else {
      return value;
    }
  }

  /**
   * Whether the list holds a query row or a record, such as the result of a subquery. Neither has an order of its own, so
   * comparing two of them directly fails (issue #12881).
   */
  private static boolean holdsRows(final List<Object> list) {
    for (final Object item : list)
      if (item instanceof Result || item instanceof Document)
        return true;
    return false;
  }

  /**
   * Sorts rows and records by their properties, the order the same rows written as maps get, and answers the rows themselves.
   * Each key is built once per row rather than once per comparison, since building one copies the row's properties.
   */
  private static void sortRows(final List<Object> list, final boolean descending) {
    final int size = list.size();
    final Object[] items = list.toArray();
    final Object[] keys = new Object[size];
    final Integer[] order = new Integer[size];
    for (int i = 0; i < size; i++) {
      keys[i] = sortKey(items[i]);
      order[i] = i;
    }
    if (descending)
      Arrays.sort(order, (left, right) -> BinaryComparator.compareTo(keys[right], keys[left]));
    else
      Arrays.sort(order, (left, right) -> BinaryComparator.compareTo(keys[left], keys[right]));
    for (int i = 0; i < size; i++)
      list.set(i, items[order[i]]);
  }

  private static Object sortKey(final Object item) {
    if (item instanceof Result row) {
      final Map<String, Object> key = new HashMap<>();
      for (final String name : row.getPropertyNames())
        key.put(name, row.getProperty(name));
      return key;
    }
    if (item instanceof Document document)
      return document.toMap(false);
    return item;
  }

  @Override
  public String getSyntax() {
    return "sort(<bool>)";
  }
}
