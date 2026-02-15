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
package com.arcadedb.query.opencypher.functions.coll;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * coll.indexOf(list, value) - Returns the index of the first occurrence of value in the list, or -1 if not found.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollIndexOf extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "indexOf";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Returns the index of the first occurrence of value in the list, or -1 if not found";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 2)
      throw new CommandExecutionException("coll.indexOf() requires exactly 2 arguments");
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;
    return (long) list.indexOf(args[1]);
  }
}
