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
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.serializer.BinaryComparator;

import java.util.ArrayList;
import java.util.List;

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
      if (params != null && params.length > 0 && params[0] instanceof Boolean bool && !bool)
        result.sort((left, right) -> BinaryComparator.compareTo(right, left));
      else
        result.sort((left, right) -> BinaryComparator.compareTo(left, right));
      return result;
    } else {
      return value;
    }
  }

  @Override
  public String getSyntax() {
    return "sort(<bool>)";
  }
}
