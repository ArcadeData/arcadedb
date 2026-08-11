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

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class BackupSettings {
  /**
   * Upper bound on the compression thread count, matching the allowed-value set of
   * {@link com.arcadedb.GlobalConfiguration#BACKUP_COMPRESSION_THREADS} so the CLI, the API, SQL and the global
   * configuration all reject the same values. Far above any core count that makes sense; the point is to bound the
   * pool, not to be a useful setting up there.
   * <p>
   * The engine module cannot depend on this one, so {@code BACKUP_COMPRESSION_THREADS} repeats the literal instead of
   * referencing this constant. Change one and change the other; {@code backupThreadBoundMatchesTheGlobalConfiguration}
   * fails if they drift.
   */
  public static final int MAX_COMPRESSION_THREADS = 256;

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
  /**
   * Deflate level, 0 (store) to 9 (smallest). {@code null} defers to {@link com.arcadedb.GlobalConfiguration#BACKUP_COMPRESSION_LEVEL}.
   */
  public       Integer             compressionLevel;
  /**
   * Compression threads: -1 automatic, 0 legacy single-threaded writer, N a pool of N. {@code null} defers to
   * {@link com.arcadedb.GlobalConfiguration#BACKUP_COMPRESSION_THREADS}.
   */
  public       Integer             compressionThreads;
  /**
   * Read-side rate cap in MB/s, 0 for unlimited. {@code null} defers to
   * {@link com.arcadedb.GlobalConfiguration#BACKUP_MAX_MB_PER_SECOND}.
   */
  public       Integer             maxMBPerSecond;

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length; )
        i += parseParameter(args[i].substring(1), i < args.length - 1 ? args[i + 1] : null);

    validateSettings();
  }

  public void validateSettings() {
    if (format == null)
      throw new IllegalArgumentException("Missing backup format");

    if (file != null && (file.startsWith("http://") || file.startsWith("https://") || file.startsWith("classpath://")))
      throw new IllegalArgumentException(
          "Backup to remote URLs is not supported. Only local file paths and 'file://' URLs are allowed");

    if (directory != null && file != null) {
      final String f = file.startsWith("file://") ? file.substring("file://".length()) : file;
      if (f.contains("..") || f.contains(File.separator))
        throw new IllegalArgumentException("Backup file cannot contain path change because the directory is specified");
    }

    if (file == null)
      // ASSIGN DEFAULT FILENAME
      if ("full".equals(format)) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
        file = "%s-backup-%s.zip".formatted(databaseName, dateFormat.format(System.currentTimeMillis()));
      }
  }

  public int parseParameter(final String name, final String value) {
    switch (name) {
    case "encryptionAlgorithm" -> encryptionAlgorithm = value;
    case "encryptionKey" -> encryptionKey = value;
    case "compressionLevel" -> compressionLevel = parseIntSetting(name, value, 0, 9);
    case "compressionThreads" -> compressionThreads = parseIntSetting(name, value, -1, MAX_COMPRESSION_THREADS);
    case "maxMBPerSecond" -> maxMBPerSecond = parseIntSetting(name, value, 0, Integer.MAX_VALUE);
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

    // ADDITIONAL OPTIONS
    default -> options.put(name, value);
    }
    return 2;
  }

  /**
   * Rejects an out-of-range or non-numeric value at parse time instead of letting it reach the compressor, where an
   * invalid deflate level would surface as an opaque {@code IllegalArgumentException} from the JDK.
   */
  public static int parseIntSetting(final String name, final String value, final int min, final int max) {
    final int parsed;
    try {
      parsed = Integer.parseInt(value != null ? value.trim() : null);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Backup setting '%s' requires an integer, found '%s'".formatted(name, value), e);
    }
    return checkIntSetting(name, parsed, min, max);
  }

  public static int checkIntSetting(final String name, final int value, final int min, final int max) {
    if (value < min || value > max)
      throw new IllegalArgumentException(
          "Backup setting '%s' must be between %d and %d, found %d".formatted(name, min, max, value));
    return value;
  }
}
