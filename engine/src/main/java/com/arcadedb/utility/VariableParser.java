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
package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.util.logging.*;

/**
 * Resolve entity class and descriptors using the paths configured.
 *
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class VariableParser {
  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd,
      final VariableParserListener iListener) {
    return resolveVariables(iText, iBegin, iEnd, iListener, null);
  }

  public static Object resolveVariables(final String text, final String beginPattern, final String endPattern,
      final VariableParserListener listener, final Object defaultValue) {
    if (listener == null)
      throw new IllegalArgumentException("Missed VariableParserListener listener");

    final int beginPos = text.lastIndexOf(beginPattern);
    if (beginPos == -1)
      return text;

    final int endPos = text.indexOf(endPattern, beginPos + 1);
    if (endPos == -1)
      return text;

    final String pre = text.substring(0, beginPos);
    String var = text.substring(beginPos + beginPattern.length(), endPos);
    final String post = text.substring(endPos + endPattern.length());

    // DECODE INTERNAL
    var = var.replace("$\\{", "${");
    var = var.replace("\\}", "}");

    Object resolved = listener.resolve(var);

    if (resolved == null) {
      if (defaultValue == null)
        LogManager.instance().log(null, Level.INFO, "Error on resolving property: %s", var);
      else
        resolved = defaultValue;
    }

    if (!pre.isEmpty() || !post.isEmpty()) {
      final String path = pre + (resolved != null ? resolved.toString() : "") + post;
      return resolveVariables(path, beginPattern, endPattern, listener);
    }

    return resolved;
  }
}
