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

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HexFormat;

/**
 * text.hexValue(value) - Upper-case hexadecimal of a number, zero-padded as {@code apoc.text.hexValue} pads it (issue #12104): 4 digits
 * up to 0xFFFF, 8 digits up to 0xFFFFFFFF and for a negative number in the int range (two's complement: -1 is {@code 'FFFFFFFF'}),
 * 16 digits for the rest. A string is written one UTF-16 unit at a time and a byte array one byte at a time.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextHexValue extends AbstractTextFunction {
  private static final HexFormat HEX = HexFormat.of().withUpperCase();

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
    return "Convert value to upper-case hexadecimal, zero-padded to 4, 8 or 16 digits";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (args[0] instanceof Number) {
      // Issue #10850: a whole number - longValue() rendered 255.9 as "ff"
      final long value = CypherFunctionHelper.requireWholeNumberArgument(args[0], getName(), "value");
      // APOC writes a number below the int range as the 4 or 8 low digits of its int value (-4294967296 is "0000"), and all 16 here
      if (value > 0xFFFFFFFFL || value < Integer.MIN_VALUE)
        return HEX.toHexDigits(value);
      if (value > 0xFFFFL || value < 0)
        return HEX.toHexDigits((int) value);
      return HEX.toHexDigits((short) value);
    }

    if (args[0] instanceof byte[] bytes)
      return HEX.formatHex(bytes);

    // For strings, convert each character to hex
    final String str = asString(args[0]);
    final StringBuilder hex = new StringBuilder(str.length() * 4);
    for (int i = 0; i < str.length(); i++)
      hex.append(HEX.toHexDigits(str.charAt(i)));
    return hex.toString();
  }
}
