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
package com.arcadedb.function.convert;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * convert.toList(value) - Convert to list.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class ConvertToList extends AbstractConvertFunction {
  @Override
  protected String getSimpleName() {
    return "toList";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Convert value to a list";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (args[0] instanceof List<?> list) {
      return new ArrayList<>(list);
    }

    if (args[0] instanceof Collection<?> collection) {
      return new ArrayList<>(collection);
    }

    if (args[0] instanceof Iterator<?> iter) {
      final List<Object> result = new ArrayList<>();
      while (iter.hasNext()) {
        result.add(iter.next());
      }
      return result;
    }

    if (args[0] instanceof Iterable<?> iterable) {
      final List<Object> result = new ArrayList<>();
      for (final Object item : iterable) {
        result.add(item);
      }
      return result;
    }

    // Single value - wrap in list
    return List.of(args[0]);
  }
}
