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
package com.arcadedb.integration.backup;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class BackupSettings {
  public       String              format        = "full";
  public       String              databaseURL;
  public       String              directory;
  public       String              file;
  public       boolean             overwriteFile = false;
  public       int                 verboseLevel  = 2;
  public final Map<String, String> options       = new HashMap<>();
  public       String              databaseName;

  public BackupSettings() {
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length; )
        i += parseParameter(args[i].substring(1), i < args.length - 1 ? args[i + 1] : null);

    validateSettings();
  }

  public void validateSettings() {
    if (format == null)
      throw new IllegalArgumentException("Missing backup format");

    if (directory != null && file != null) {
      final String f = file.startsWith("file://") ? file.substring("file://".length()) : file;
      if (f.contains("..") || f.contains("/"))
        throw new IllegalArgumentException("Backup file cannot contain path change because the directory is specified");
    }

    if (file == null)
      // ASSIGN DEFAULT FILENAME
      switch (format) {
      case "full":
        final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
        file = String.format("%s-backup-%s.zip", databaseName, dateFormat.format(System.currentTimeMillis()));
        break;
      }
  }

  public int parseParameter(final String name, final String value) {
    if ("format".equals(name)) {
      if (value != null)
        format = value.toLowerCase();
    } else if ("dir".equals(name)) {
      if (value != null)
        directory = value.endsWith("/") ? value : value + "/";
    } else if ("f".equals(name)) {
      if (value != null)
        file = value;
    } else if ("d".equals(name)) {
      if (value != null)
        databaseURL = value;
    } else if ("o".equals(name)) {
      overwriteFile = true;
      return 1;
    } else
      // ADDITIONAL OPTIONS
      options.put(name, value);
    return 2;
  }
}
