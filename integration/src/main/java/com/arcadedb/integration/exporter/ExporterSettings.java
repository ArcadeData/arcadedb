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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class ExporterSettings {
  public       String              format;
  public       String              databaseURL;
  public       String              file;
  public       boolean             overwriteFile = false;
  public       int                 verboseLevel  = 2;
  public final Map<String, String> options       = new HashMap<>();

  public ExporterSettings() {
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; )
        i += parseParameter(args[i].substring(1), args[i + 1]);

    if (format == null)
      throw new IllegalArgumentException("Missing export format");

    if (file == null)
      // ASSIGN DEFAULT FILENAME
      switch (format) {
      case "jsonl":
        file = "arcadedb-export-%s.jsonl.tgz";
        break;
      case "backup":
        file = "arcadedb-backup-%s.zip";
        break;
      }

    if (file == null) {
      final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
      file = String.format(file, dateFormat.format(System.currentTimeMillis()));
    }
  }

  public int parseParameter(final String name, final String value) {
    if ("format".equals(name))
      format = value.toLowerCase();
    else if ("f".equals(name))
      file = value;
    else if ("d".equals(name))
      databaseURL = value;
    else if ("o".equals(name)) {
      overwriteFile = true;
      return 1;
    } else
      // ADDITIONAL OPTIONS
      options.put(name, value);
    return 2;
  }
}
