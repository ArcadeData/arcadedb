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
package com.arcadedb.query.sql.function.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.*;

/**
 * Compute the mode (or multimodal) value for a field. The scores in the field's distribution that occurs more frequently. Nulls are
 * ignored in the calculation.
 *
 * @author Fabrizio Fortino
 */
public class SQLFunctionMode extends SQLFunctionAbstract {

  public static final String NAME = "mode";

  private final Map<Object, Integer> seen     = new HashMap<Object, Integer>();
  private       int                  max      = 0;
  private final List<Object>         maxElems = new ArrayList<Object>();

  public SQLFunctionMode() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {

    if (MultiValue.isMultiValue(params[0])) {
      for (final Object o : MultiValue.getMultiValueIterable(params[0])) {
        max = evaluate(o, 1, seen, maxElems, max);
      }
    } else {
      max = evaluate(params[0], 1, seen, maxElems, max);
    }
    return getResult();
  }

  @Override
  public Object getResult() {
    return maxElems.isEmpty() ? null : maxElems;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  private int evaluate(final Object value, final int times, final Map<Object, Integer> iSeen, final List<Object> iMaxElems, int iMax) {
    if (value != null) {
      if (iSeen.containsKey(value)) {
        iSeen.put(value, iSeen.get(value) + times);
      } else {
        iSeen.put(value, times);
      }
      if (iSeen.get(value) > iMax) {
        iMax = iSeen.get(value);
        iMaxElems.clear();
        iMaxElems.add(value);
      } else if (iSeen.get(value) == iMax) {
        iMaxElems.add(value);
      }
    }
    return iMax;
  }

}
