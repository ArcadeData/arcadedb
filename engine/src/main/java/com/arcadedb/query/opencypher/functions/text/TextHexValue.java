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
package com.arcadedb.query.opencypher.functions.text;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.hexValue(value) - Convert to hexadecimal string.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextHexValue extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "hexValue";
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
    return "Convert value to hexadecimal string representation";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (args[0] instanceof Number) {
      return Long.toHexString(((Number) args[0]).longValue());
    }

    if (args[0] instanceof byte[]) {
      final byte[] bytes = (byte[]) args[0];
      final StringBuilder hex = new StringBuilder(bytes.length * 2);
      for (final byte b : bytes) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    }

    // For strings, convert each character to hex
    final String str = asString(args[0]);
    final StringBuilder hex = new StringBuilder(str.length() * 4);
    for (int i = 0; i < str.length(); i++) {
      hex.append(String.format("%04x", (int) str.charAt(i)));
    }
    return hex.toString();
  }
}
