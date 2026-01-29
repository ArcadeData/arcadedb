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
package com.arcadedb.query.opencypher.functions.convert;

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.Map;

/**
 * convert.toMap(value) - Convert to map.
 *
 * @author ArcadeDB Team
 */
public class ConvertToMap extends AbstractConvertFunction {
  @Override
  protected String getSimpleName() {
    return "toMap";
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
    return "Convert value to a map";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (args[0] instanceof Map) {
      return new HashMap<>((Map<String, Object>) args[0]);
    }

    if (args[0] instanceof Document) {
      final Document doc = (Document) args[0];
      final Map<String, Object> result = new HashMap<>();
      for (final String prop : doc.getPropertyNames()) {
        result.put(prop, doc.get(prop));
      }
      return result;
    }

    throw new IllegalArgumentException("convert.toMap() cannot convert " + args[0].getClass().getSimpleName() + " to map");
  }
}
