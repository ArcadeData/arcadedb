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

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class RestoreSettings {
  /**
   * Upper bound on the restore thread count, matching the allowed-value set of
   * {@link com.arcadedb.GlobalConfiguration#RESTORE_THREADS} so the CLI, the API and the global configuration all
   * reject the same values. Far above any core count that makes sense; the point is to bound the pool, not to be a
   * useful setting up there.
   * <p>
   * The engine module cannot depend on this one, so {@code RESTORE_THREADS} repeats the literal instead of
   * referencing this constant. Change one and change the other; {@code restoreThreadBoundMatchesTheGlobalConfiguration}
   * fails if they drift.
   */
  public static final int MAX_RESTORE_THREADS = 256;

  public       String              format               = "full";
  public       String              inputFileURL;
  public       String              databaseDirectory;
  public       boolean             overwriteDestination = false;
  public       int                 verboseLevel         = 2;
  public       String              encryptionAlgorithm  = "AES";
  public       String              encryptionKey;
  /**
   * Restore threads: -1 automatic, 0 the legacy single-threaded stream walk, N a pool of N. {@code null} defers to
   * {@link com.arcadedb.GlobalConfiguration#RESTORE_THREADS}.
   */
  public       Integer             restoreThreads;
  /**
   * Whether an http(s) input URL may resolve to a local-file, loopback, link-local or private-network address.
   * {@code null} defers to {@link com.arcadedb.GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} (the
   * default, and what a CLI/embedded invocation with no server gets); a caller that already resolved this against
   * its own configuration - the server command handler, against its per-instance {@code ContextConfiguration} - sets
   * it explicitly so the fetch-time check agrees with whatever pre-check already accepted the command (issue #6381).
   */
  public       Boolean             allowLocalUrls;
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
      case "restoreThreads" -> {
        restoreThreads = parseIntSetting(name, value, -1, MAX_RESTORE_THREADS);
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

  /**
   * Rejects an out-of-range or non-numeric value at parse time instead of letting it reach the executor, where an
   * invalid thread count would surface as an opaque {@code IllegalArgumentException} from the JDK.
   */
  public static int parseIntSetting(final String name, final String value, final int min, final int max) {
    final int parsed;
    try {
      parsed = Integer.parseInt(value != null ? value.trim() : null);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Restore setting '%s' requires an integer, found '%s'".formatted(name, value), e);
    }
    return checkIntSetting(name, parsed, min, max);
  }

  public static int checkIntSetting(final String name, final int value, final int min, final int max) {
    if (value < min || value > max)
      throw new IllegalArgumentException(
          "Restore setting '%s' must be between %d and %d, found %d".formatted(name, min, max, value));
    return value;
  }
}
