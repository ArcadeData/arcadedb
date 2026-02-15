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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * toStringList(list) - converts all elements in a list to strings.
 * Non-convertible elements become null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ToStringListFunction implements StatelessFunction {
  private final ToStringFunction converter = new ToStringFunction();

  @Override
  public String getName() {
    return "toStringList";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("toStringList() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (!(args[0] instanceof List))
      throw new CommandExecutionException("toStringList() requires a list argument");

    final List<?> input = (List<?>) args[0];
    final List<Object> result = new ArrayList<>(input.size());
    for (final Object item : input) {
      if (item == null)
        result.add(null);
      else {
        try {
          result.add(converter.execute(new Object[] { item }, context));
        } catch (final Exception e) {
          result.add(null);
        }
      }
    }
    return result;
  }
}
