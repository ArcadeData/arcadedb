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
package com.arcadedb.function.agg;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * agg.first(list) - Return the first non-null element in the list.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AggFirst extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "first";
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
    return "Return the first non-null element in the list";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final List<Object> list = toObjectList(args[0]);
    for (final Object item : list) {
      if (item != null)
        return item;
    }
    return null;
  }
}
