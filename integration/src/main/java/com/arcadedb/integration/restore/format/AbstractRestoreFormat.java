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
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.ProgressCallback;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public abstract class AbstractRestoreFormat {
  /**
   * The single step a restore format reports as, in the numbering the format itself owns (step 1 of 1). A caller
   * that wraps the restore in a larger operation - the server's control plane, which also has a swap and a
   * replicate phase - renumbers it on the way through rather than asking the format to know about phases it
   * cannot see (issue #7385).
   */
  public static final String RESTORE_STEP_NAME = "Restoring files";

  protected final        RestoreSettings   settings;
  protected final        DatabaseInternal  database;
  protected final        ConsoleLogger     logger;
  protected static final DateTimeFormatter dateFormat = DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  /** Where the entry counters go, or null when the caller asked for none. */
  protected ProgressCallback progressCallback;

  protected AbstractRestoreFormat(final DatabaseInternal database, final RestoreSettings settings, final ConsoleLogger logger) {
    this.database = database;
    this.settings = settings;
    this.logger = logger;
  }

  public void setProgressCallback(final ProgressCallback progressCallback) {
    this.progressCallback = progressCallback;
  }

  /**
   * Publishes how many archive entries are done out of how many there are, or out of {@code -1} when the path
   * taken cannot know the denominator. A no-op when nobody asked for progress, which is the common case (the CLI
   * and every embedded caller).
   */
  protected void reportRestoreProgress(final long done, final long total) {
    // READ THE FIELD ONCE: the callback is installed before the restore starts, but reading it twice would still
    // be two chances to see a null that the null check already passed.
    final ProgressCallback callback = progressCallback;
    if (callback != null)
      callback.onProgress(RESTORE_STEP_NAME, 1, 1, done, total);
  }

  public abstract void restoreDatabase() throws Exception;
}
