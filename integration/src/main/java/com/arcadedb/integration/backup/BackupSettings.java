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

import java.io.*;
import java.text.*;
import java.util.*;

public class BackupSettings {
  public       String              format              = "full";
  public       String              databaseURL;
  public       String              directory;
  public       String              file;
  public       boolean             overwriteFile       = false;
  public       int                 verboseLevel        = 2;
  public       String              encryptionAlgorithm = "AES";
  public       String              encryptionKey;
  public final Map<String, String> options             = new HashMap<>();
  public       String              databaseName;

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
      if (f.contains("..") || f.contains(File.separator))
        throw new IllegalArgumentException("Backup file cannot contain path change because the directory is specified");
    }

    if (file == null)
      // ASSIGN DEFAULT FILENAME
      if (format.equals("full")) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
        file = "%s-backup-%s.zip".formatted(databaseName, dateFormat.format(System.currentTimeMillis()));
      }
  }

  public int parseParameter(final String name, final String value) {
    switch (name) {
    case "encryptionAlgorithm" -> encryptionAlgorithm = value;
    case "encryptionKey" -> encryptionKey = value;
    case "format" -> {
      if (value != null)
        format = value.toLowerCase(Locale.ENGLISH);
    }
    case "dir" -> {
      if (value != null)
        directory = value.endsWith(File.separator) ? value : value + File.separator;
    }
    case "f" -> {
      if (value != null)
        file = value;
    }
    case "d" -> {
      if (value != null)
        databaseURL = value;
    }
    case "o" -> {
      overwriteFile = true;
      return 1;
    }

    case null, default ->
      // ADDITIONAL OPTIONS
        options.put(name, value);
    }
    return 2;
  }
}
