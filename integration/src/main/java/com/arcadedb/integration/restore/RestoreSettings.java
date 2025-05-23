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
package com.arcadedb.integration.restore;

import java.io.*;
import java.util.*;

public class RestoreSettings {
  public       String              format               = "full";
  public       String              inputFileURL;
  public       String              databaseDirectory;
  public       boolean             overwriteDestination = false;
  public       int                 verboseLevel         = 2;
  public       String              encryptionAlgorithm  = "AES";
  public       String              encryptionKey;
  public final Map<String, String> options              = new HashMap<>();

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length; )
        i += parseParameter(args[i].substring(1), i < args.length - 1 ? args[i + 1] : null);

    validate();
  }

  public int parseParameter(final String name, final String value) {
    return switch (name) {
      case "encryptionAlgorithm" -> {
        encryptionAlgorithm = value;
        yield 2;
      }
      case "encryptionKey" -> {
        encryptionKey = value;
        yield 2;
      }
      case "format" -> {
        if (value != null)
          format = value.toLowerCase(Locale.ENGLISH);
        yield 2;
      }
      case "f" -> {
        if (value != null)
          inputFileURL = value;
        yield 2;
      }
      case "d" -> {
        if (value != null)
          databaseDirectory = value;
        yield 2;
      }
      case "o" -> {
        overwriteDestination = true;
        yield 1;
      }
      default -> {
        // ADDITIONAL OPTIONS
        options.put(name, value);
        yield 2;
      }
    };
  }

  public void validate() {
    if (format == null)
      throw new IllegalArgumentException("Missing backup format");

    if (inputFileURL == null)
      throw new IllegalArgumentException("Missing input file url. Use -f <input-file-url>");

    if (databaseDirectory == null)
      throw new IllegalArgumentException("Missing database url. Use -d <database-directory>");

    if (inputFileURL.contains("..") || inputFileURL.startsWith(File.separator))
      throw new IllegalArgumentException("Invalid backup file: cannot contain '..' or start with '/'");
  }
}
