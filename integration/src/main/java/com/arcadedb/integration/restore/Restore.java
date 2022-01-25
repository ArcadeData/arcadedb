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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.format.AbstractRestoreFormat;
import com.arcadedb.integration.restore.format.FullRestoreFormat;

import java.util.Timer;

public class Restore {
  protected RestoreSettings       settings = new RestoreSettings();
  protected DatabaseInternal      database;
  protected Timer                 timer;
  protected ConsoleLogger         logger;
  protected AbstractRestoreFormat formatImplementation;

  public Restore(final String[] args) {
    settings.parseParameters(args);
  }

  public Restore(final String file, final String databaseURL) {
    settings.inputFileURL = file;
    settings.databaseDirectory = databaseURL;
  }

  public static void main(final String[] args) {
    new Restore(args).restoreDatabase();
    System.exit(0);
  }

  public void restoreDatabase() {
    try {
      if (logger == null)
        logger = new ConsoleLogger(settings.verboseLevel);

      formatImplementation = createFormatImplementation();
      formatImplementation.restoreDatabase();

    } catch (Exception e) {
      throw new RestoreException("Error during restore of database from file '" + settings.inputFileURL + "'", e);
    }
  }

  public Restore setVerboseLevel(final int verboseLevel) {
    settings.verboseLevel = verboseLevel;
    return this;
  }

  protected AbstractRestoreFormat createFormatImplementation() {
    switch (settings.format.toLowerCase()) {
    case "full":
      return new FullRestoreFormat(database, settings, logger);

    default:
      throw new RestoreException("Format '" + settings.format + "' not supported");
    }
  }
}
