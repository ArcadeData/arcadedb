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

import java.util.Locale;

/**
 * text.upperCamelCase(string) - Convert to UpperCamelCase (PascalCase): text.camelCase() with the first character upper case, as
 * {@code apoc.text.upperCamelCase} does (issue #12104).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextUpperCamelCase extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "upperCamelCase";
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
    return "Convert string to UpperCamelCase (PascalCase): camelCase with the first character upper case";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String camelCase = TextCamelCase.camelCase(str);
    if (camelCase.isEmpty())
      return camelCase;

    // the first character is a code point (issue #10484)
    final int firstLength = Character.charCount(camelCase.codePointAt(0));
    return camelCase.substring(0, firstLength).toUpperCase(Locale.ROOT) + camelCase.substring(firstLength);
  }
}
