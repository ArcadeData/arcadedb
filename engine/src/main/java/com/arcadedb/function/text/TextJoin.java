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
package com.arcadedb.function.text;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * text.join(list, delimiter) - Join list elements into a string.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextJoin extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "join";
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
    return "Join list elements into a string with the specified delimiter";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (!(args[0] instanceof List)) {
      throw new IllegalArgumentException("text.join() first argument must be a list");
    }

    final List<?> list = (List<?>) args[0];
    final String delimiter = asString(args[1]);

    return list.stream()
        .map(item -> item == null ? "" : item.toString())
        .collect(Collectors.joining(delimiter == null ? "" : delimiter));
  }
}
