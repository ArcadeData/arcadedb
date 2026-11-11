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

import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StringUtils;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

public class ExporterSettings {
  /**
   * Every setting {@link #parseParameter} accepts, which is what a {@code WITH} clause of {@code EXPORT DATABASE} may carry. It
   * used to accept anything, into a free-form map nothing read back, so a misspelled setting ({@code exludeTypes}) exported
   * everything and answered OK (issue #11523). The target file and the database are not among them: the caller fixes both,
   * and only the command line sets them ({@link #parseParameters}).
   */
  public static final Set<String> SETTINGS = Set.of("format", "overwrite", "includeTypes", "excludeTypes", "includeRecords");

  public String      format;
  public String      databaseURL;
  public String      file;
  public boolean     overwriteFile = false;
  public int         verboseLevel  = 2;
  public Set<String> includeRecords;
  public Set<String> includeTypes;
  public Set<String> excludeTypes;

  /**
   * Parses the command line: {@code -o} is a flag with no value, every other argument is a {@code -<name> <value>} pair. A
   * trailing {@code -o} used to be dropped, because the loop stopped one argument before the end expecting a value for every
   * name (issue #11523).
   */
  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length; ) {
        if (args[i].isEmpty() || args[i].charAt(0) != '-')
          throw new IllegalArgumentException("Arguments must begin with '-', found '" + args[i] + "'");

        final String name = args[i].substring(1);
        if ("o".equals(name)) {
          overwriteFile = true;
          ++i;
          continue;
        }

        if (i + 1 >= args.length)
          throw new IllegalArgumentException("Missing value for -" + name);

        final String value = args[i + 1];
        switch (name) {
        case "f", "file" -> file = value;
        case "d" -> databaseURL = value;
        default -> parseParameter(name, value);
        }
        i += 2;
      }

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

    file = file.formatted(LocalDateTime.now().format(DateUtils.FILE_NAME_TIMESTAMP));
  }

  /**
   * Applies one of the {@link #SETTINGS}. {@code overwrite} takes its value, strictly {@code true} or {@code false}: it used to
   * be set by its name alone, so {@code WITH overwrite = false} replaced an existing export like {@code overwrite = true}
   * (issue #11523).
   *
   * @throws IllegalArgumentException for a name that is not one of the {@link #SETTINGS}, or a value the setting cannot take
   */
  public void parseParameter(String name, final String value) {
    name = FileUtils.getStringContent(name);

    switch (name) {
    case "format" -> format = value.toLowerCase(Locale.ENGLISH);
    case "overwrite" -> {
      try {
        overwriteFile = StringUtils.parseStrictBoolean(value);
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid value '" + value + "' for -overwrite. Supported values are 'true' and 'false'");
      }
    }
    case "includeTypes" -> includeTypes = Set.of(value.split(","));
    case "excludeTypes" -> excludeTypes = Set.of(value.split(","));
    case "includeRecords" -> includeRecords = Set.of(value.split(","));
    default -> {
      final String closest = StringUtils.closestMatch(name, SETTINGS);
      throw new IllegalArgumentException("Unknown export setting '" + name + "'. " + (closest != null ?
          "Did you mean '" + closest + "'?" :
          "Supported settings: " + new TreeSet<>(SETTINGS)));
    }
    }
  }
}
