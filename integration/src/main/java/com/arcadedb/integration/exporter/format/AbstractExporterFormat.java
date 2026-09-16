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
package com.arcadedb.integration.exporter.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.exporter.ExportException;
import com.arcadedb.integration.exporter.ExporterContext;
import com.arcadedb.integration.exporter.ExporterSettings;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.DateUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;

public abstract class AbstractExporterFormat {
  protected final        ExporterSettings  settings;
  protected final        ExporterContext   context;
  protected final        DatabaseInternal  database;
  protected final        ConsoleLogger     logger;
  protected static final DateTimeFormatter dateFormat = DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  protected AbstractExporterFormat(final DatabaseInternal database, final ExporterSettings settings, final ExporterContext context, final ConsoleLogger logger) {
    this.database = database;
    this.settings = settings;
    this.context = context;
    this.logger = logger;
  }

  public abstract void exportDatabase() throws Exception;

  public abstract String getName();

  /**
   * Claims {@code exportFile} for the duration of this export, so a second export racing to the same resolved
   * target - the same explicit URL twice, or two default names resolving within one millisecond - fails outright
   * instead of interleaving its output with this writer's (issue #7644).
   * <p>
   * {@code MaintenanceCoordinator.Operation.EXPORT} admits any number of exports of one database together (issue
   * #7450), on the premise that two exports name two different files. Nothing enforced that premise: {@code
   * file.exists() && !overwriteFile} is a check-then-create a second export starting in the same instant can both
   * pass, and with {@code overwriteFile} set the check does not even apply, so two writers simply interleaved into
   * one archive.
   * <p>
   * A sidecar lock rather than an atomic create of {@code exportFile} itself ({@code FullBackupFormat}'s
   * {@code claimBackupFile} does exactly that for backups): the target legitimately pre-exists here whenever
   * {@code overwriteFile} is set, so claiming the PATH - not the absence of a file at it - is what is needed, and
   * that is what a lock file gives independently of {@code overwriteFile}.
   *
   * @return the lock file; release it with {@link #releaseExportFile(File)} from a {@code finally}
   */
  protected final File claimExportFile(final File exportFile) {
    final File lock = new File(exportFile.getPath() + ".exporting");
    try {
      Files.createFile(lock.toPath());
    } catch (final FileAlreadyExistsException e) {
      throw new ExportException("Another export to '%s' is already in progress".formatted(exportFile));
    } catch (final IOException e) {
      throw new ExportException("Export target '%s' cannot be claimed".formatted(exportFile), e);
    }
    return lock;
  }

  /**
   * Releases the claim {@link #claimExportFile(File)} took. Always call it from a {@code finally}: a leaked lock
   * file blocks every later export to that target until an operator removes it by hand - which is exactly why a
   * failed delete is logged rather than left silent: the log line is what points an operator at the file to
   * remove, instead of leaving them to discover the block only on the next export attempt.
   */
  protected final void releaseExportFile(final File lock) {
    if (!lock.delete())
      LogManager.instance().log(this, Level.WARNING,
          "Could not delete the export lock file '%s': later exports to the same target will be refused until it is removed",
          null, lock);
  }

  /**
   * Creates {@code file}'s parent directory if it does not exist yet, tolerating a concurrent creation by another
   * export racing to the same parent - the same class of hazard {@link #claimExportFile} closes for the target
   * file itself, one level up (issue #7644 follow-up).
   * <p>
   * {@code File.mkdirs()} is a check-then-create that is not atomic across threads or processes: the first
   * {@code EXPORT DATABASE} of a database whose {@code exports/} directory does not exist yet is exactly the
   * case {@code Operation.EXPORT} admits several of at once (issue #7450), so several callers can see the
   * directory absent, race to create it, and one of them gets {@code mkdirs() == false} even though the
   * directory exists by the time it checks - not because anything is actually wrong. {@link Files#createDirectories}
   * does not have that failure mode: it treats an already-existing directory as success rather than as a race
   * it lost.
   */
  protected final void ensureParentDirectory(final File file) {
    final File parent = file.getParentFile();
    if (parent == null)
      return;
    try {
      Files.createDirectories(parent.toPath());
    } catch (final IOException e) {
      throw new ExportException("The export file '%s' cannot be created".formatted(file), e);
    }
  }
}
