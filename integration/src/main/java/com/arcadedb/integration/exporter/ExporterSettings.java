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
package com.arcadedb.integration.exporter;

import com.arcadedb.utility.FileUtils;

import java.text.*;
import java.util.*;

public class ExporterSettings {
  public       String              format;
  public       String              databaseURL;
  public       String              file;
  public       boolean             overwriteFile = false;
  public       int                 verboseLevel  = 2;
  public       Set<String>         includeTypes;
  public       Set<String>         excludeTypes;
  public final Map<String, String> options       = new HashMap<>();

  public ExporterSettings() {
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; )
        i += parseParameter(args[i].substring(1), args[i + 1]);

    if (includeTypes != null && excludeTypes != null)
      throw new IllegalArgumentException("Both includeTypes and excludeTypes were defined, but they are mutual exclusive");

    if (format == null)
      throw new IllegalArgumentException("Missing export format");

    if (file == null)
      // ASSIGN DEFAULT FILENAME
      switch (format) {
      case "backup":
        file = "arcadedb-backup-%s.zip";
        break;
      default:
        file = "arcadedb-backup-%s." + format + ".tgz";
        break;
      }

    final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
    file = String.format(file, dateFormat.format(System.currentTimeMillis()));
  }

  public int parseParameter(String name, final String value) {
    name = FileUtils.getStringContent(name);

    if ("format".equals(name))
      format = value.toLowerCase();
    else if ("f".equals(name) || "file".equals(name))
      file = value;
    else if ("d".equals(name))
      databaseURL = value;
    else if ("overwrite".equals(name))
      overwriteFile = true;
    else if ("o".equals(name)) {
      overwriteFile = true;
      return 1;
    } else if ("includeTypes".equals(name))
      includeTypes = Set.of(value.split(","));
    else if ("excludeTypes".equals(name))
      excludeTypes = Set.of(value.split(","));
    else
      // ADDITIONAL OPTIONS
      options.put(name, value);
    return 2;
  }
}
