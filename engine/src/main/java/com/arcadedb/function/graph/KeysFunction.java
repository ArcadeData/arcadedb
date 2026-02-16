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
package com.arcadedb.function.graph;

import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.*;

/**
 * keys() function - returns the property keys of a node or relationship.
 */
public class KeysFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "keys";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1) {
      throw new CommandExecutionException("keys() requires exactly one argument");
    }
    if (args[0] == null)
      return null;
    if (args[0] instanceof Document) {
      final Document doc = (Document) args[0];
      return new ArrayList<>(doc.getPropertyNames());
    }
    if (args[0] instanceof Map)
      return new ArrayList<>(((Map<?, ?>) args[0]).keySet());
    return Collections.emptyList();
  }
}
