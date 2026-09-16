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
import java.util.Locale;
import java.util.logging.Level;

public abstract class AbstractExporterFormat {
  /** The sidecar {@link #claimExportFile(File)} writes its claim to, and therefore a name no export may target. */
  protected static final String            LOCK_SUFFIX = ".exporting";
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
   * <p>
   * The no-overwrite refusal is re-applied HERE, under the claim, and not only at the top of each format's
   * {@code exportDatabase}: checked before the claim only, export A could find the target missing, export B could
   * claim it, write a complete archive and release, and export A would then truncate B's finished archive despite
   * {@code overwriteFile} being false. Only a check that runs while this export holds the claim means anything
   * against a concurrent one (review of PR #7649).
   *
   * @return the lock file; release it with {@link #releaseExportFile(File)} from a {@code finally}
   */
  protected final File claimExportFile(final File exportFile) {
    // A TARGET NAMED LIKE A LOCK IS REFUSED RATHER THAN CLAIMED: claimExportFile("x") creates "x.exporting", so an
    // export whose own target IS "x.exporting" would open for writing the very file another export is holding its
    // claim in - and that other export deletes it on the way out, taking this one's finished archive with it. The
    // suffix is a reserved name, and saying so is both cheaper and safer than a lock namespace that merely makes
    // the collision less likely (review of PR #7649).
    //
    // CASE-INSENSITIVELY, because the filesystem underneath may be: on macOS and Windows "x.EXPORTING" and
    // "x.exporting" are the same file, so a case-sensitive refusal would pass a target that still aliases a lock.
    if (exportFile.getName().toLowerCase(Locale.ROOT).endsWith(LOCK_SUFFIX))
      throw new ExportException(
          "The export file '%s' cannot end with '%s': the suffix is reserved for the exporter's own lock files".formatted(
              exportFile, LOCK_SUFFIX));

    // FROM THE RESOLVED PATH, HERE, RATHER THAN FROM settings.file AT THE TOP OF EACH FORMAT: with a 'file://'
    // target those two differ, and creating the parent of the unresolved one leaves the real directory missing, so
    // the sidecar below could not be created and the export failed on a directory it had just "created" (review of
    // PR #7649).
    ensureParentDirectory(exportFile);

    final File lock = new File(exportFile.getPath() + LOCK_SUFFIX);
    try {
      Files.createFile(lock.toPath());
    } catch (final FileAlreadyExistsException e) {
      throw new ExportException("Another export to '%s' is already in progress".formatted(exportFile));
    } catch (final IOException e) {
      throw new ExportException("Export target '%s' cannot be claimed".formatted(exportFile), e);
    }

    // AFTER the claim, and releasing it on refusal: this is the check that is race-free, and a caller that never
    // got the claim back must not be left owing a release it cannot make.
    try {
      refuseExistingTarget(exportFile);
    } catch (final RuntimeException e) {
      releaseExportFile(lock);
      throw e;
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
   * Refuses {@code exportFile} when it already exists and the caller did not ask for an overwrite.
   * <p>
   * Called TWICE on purpose, and the second call is the one that counts. The first is a cheap fast-fail before any
   * lock file is created, for the ordinary case of an operator naming an archive that is already there. The second
   * runs INSIDE the claim {@link #claimExportFile} takes, and only that one is race-free: checked before the claim
   * only, export A could find the target missing, export B could then claim it, write a complete archive and
   * release, and export A would go on to truncate B's finished archive despite {@code overwriteFile} being false
   * (review of PR #7649). The check is meaningful against a concurrent export only while this export holds the
   * target's claim.
   */
  protected final void refuseExistingTarget(final File exportFile) {
    if (exportFile.exists() && !settings.overwriteFile)
      // The file that is actually in the way, not settings.file: for a 'file://' target the two differ, and the
      // resolved path is the one an operator has to go and look at.
      throw new ExportException("The export file '%s' already exist and '-o' setting is false".formatted(exportFile));
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
