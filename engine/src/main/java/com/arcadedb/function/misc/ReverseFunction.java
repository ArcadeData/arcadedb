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
package com.arcadedb.function.misc;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * reverse() function - reverses a string or list.
 */
public class ReverseFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "reverse";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("reverse() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String str) {
      return new StringBuilder(str).reverse().toString();
    }
    // Accept List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284).
    final List<Object> list = MultiValue.getMultiValueAsList(args[0]);
    if (list != null) {
      final List<Object> reversed = new ArrayList<>(list);
      Collections.reverse(reversed);
      return reversed;
    }
    return null;
  }
}
