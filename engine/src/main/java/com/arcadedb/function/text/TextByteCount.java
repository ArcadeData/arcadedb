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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * text.byteCount(string, [charset]) - Get byte count of string.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextByteCount extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "byteCount";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Get the byte count of string in the specified charset (default UTF-8)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    Charset charset = StandardCharsets.UTF_8;
    if (args.length > 1 && args[1] != null) {
      final String charsetName = asString(args[1]);
      charset = Charset.forName(charsetName);
    }

    return (long) str.getBytes(charset).length;
  }
}
