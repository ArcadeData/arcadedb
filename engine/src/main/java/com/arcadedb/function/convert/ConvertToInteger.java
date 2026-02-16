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

/**
 * convert.toInteger(value) - Convert to integer.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class ConvertToInteger extends AbstractConvertFunction {
  @Override
  protected String getSimpleName() {
    return "toInteger";
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
    return "Convert value to integer (long)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (args[0] instanceof Number) {
      return ((Number) args[0]).longValue();
    }

    if (args[0] instanceof Boolean) {
      return ((Boolean) args[0]) ? 1L : 0L;
    }

    if (args[0] instanceof String) {
      try {
        return Long.parseLong(((String) args[0]).trim());
      } catch (final NumberFormatException e) {
        try {
          // Try parsing as double first then truncate
          return (long) Double.parseDouble(((String) args[0]).trim());
        } catch (final NumberFormatException e2) {
          return null;
        }
      }
    }

    return null;
  }
}
