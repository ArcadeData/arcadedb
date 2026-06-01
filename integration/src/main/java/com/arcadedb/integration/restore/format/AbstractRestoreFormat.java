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
package com.arcadedb.integration.restore.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreSettings;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public abstract class AbstractRestoreFormat {
  protected final        RestoreSettings  settings;
  protected final        DatabaseInternal database;
  protected final        ConsoleLogger    logger;
  protected static final DateFormat       dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  protected AbstractRestoreFormat(final DatabaseInternal database, final RestoreSettings settings, final ConsoleLogger logger) {
    this.database = database;
    this.settings = settings;
    this.logger = logger;
  }

  public abstract void restoreDatabase() throws Exception;
}
