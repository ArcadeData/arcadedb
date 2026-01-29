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
package com.arcadedb.query.opencypher.functions.util;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * util.validate(predicate, message) - Throws an exception if the predicate is false.
 *
 * @author ArcadeDB Team
 */
public class UtilValidate extends AbstractUtilFunction {
  @Override
  protected String getSimpleName() {
    return "validate";
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
    return "Throws an exception with the given message if the predicate is false";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Object predicate = args[0];
    final String message = args[1] != null ? args[1].toString() : "Validation failed";

    final boolean valid;
    if (predicate == null) {
      valid = false;
    } else if (predicate instanceof Boolean) {
      valid = (Boolean) predicate;
    } else if (predicate instanceof String) {
      final String str = (String) predicate;
      valid = !str.isEmpty() && !str.equalsIgnoreCase("false") && !str.equals("0");
    } else if (predicate instanceof Number) {
      valid = ((Number) predicate).doubleValue() != 0.0;
    } else {
      valid = true;
    }

    if (!valid) {
      throw new IllegalArgumentException(message);
    }

    return true;
  }
}
