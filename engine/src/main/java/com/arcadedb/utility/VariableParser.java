/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * Resolve entity class and descriptors using the paths configured.
 *
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class VariableParser {
  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd, final VariableParserListener iListener) {
    return resolveVariables(iText, iBegin, iEnd, iListener, null);
  }

  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd, final VariableParserListener iListener,
      final Object iDefaultValue) {
    if (iListener == null)
      throw new IllegalArgumentException("Missed VariableParserListener listener");

    int beginPos = iText.lastIndexOf(iBegin);
    if (beginPos == -1)
      return iText;

    int endPos = iText.indexOf(iEnd, beginPos + 1);
    if (endPos == -1)
      return iText;

    String pre = iText.substring(0, beginPos);
    String var = iText.substring(beginPos + iBegin.length(), endPos);
    String post = iText.substring(endPos + iEnd.length());

    Object resolved = iListener.resolve(var);

    if (resolved == null) {
      if (iDefaultValue == null)
        LogManager.instance().log(null, Level.INFO, "[OVariableParser.resolveVariables] Error on resolving property: %s", null, var);
      else
        resolved = iDefaultValue;
    }

    if (pre.length() > 0 || post.length() > 0) {
      final String path = pre + (resolved != null ? resolved.toString() : "") + post;
      return resolveVariables(path, iBegin, iEnd, iListener);
    }

    return resolved;
  }
}
