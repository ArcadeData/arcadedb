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
package com.arcadedb.server.security;

import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

/**
 * The reload policy of a security file a running server re-reads when it changes, and the watcher that drives it (issue
 * #10404). The users ({@code server-users.jsonl}), groups ({@code server-groups.json}) and API-token
 * ({@code server-api-tokens.json}) stores each extend it, supplying what differs between them: how the file is
 * {@linkplain #read() turned into a value}, the {@linkplain #fallback() value a first load falls back to}, and how a
 * value is {@linkplain #publish published}. The policy itself lives here, once:
 * <ul>
 *   <li>a FIRST load ({@link #firstLoad()}) - nothing is in force yet - may fall back to the default when the file is
 *       missing or cannot be used;</li>
 *   <li>a RELOAD ({@link #reload()}) - something is in force - never does: a file that is missing, unreadable,
 *       unparseable or of the wrong shape keeps what is in force (issues #10326, #10383);</li>
 *   <li>a file that could not be used is preserved as the error file before anything else happens, through
 *       {@link SecurityUserFileRepository#preserveInError} (issue #10321);</li>
 *   <li>neither path writes the live file, except through {@link #publish} of a first load's fallback.</li>
 * </ul>
 * {@code Issue10383SecurityArtifactReloadConformanceTest} drives every store through the four inputs a reload can meet.
 * <p>
 * <b>Locking.</b> Every method that reads the file or publishes runs under the {@code lock} handed to the constructor,
 * which is the store's own lock: the groups and tokens stores pass their monitor, so a reload serializes with their
 * {@code synchronized} writers (save, the replicated install of #7373) exactly as it did when the policy lived in each
 * class. The one deliberately unsynchronized reader is the watcher's tick, which compares the file's stamp against the
 * {@code volatile} baseline before taking the lock.
 *
 * @param <T> what a successful read of the file produces
 */
abstract class ReloadableSecurityArtifact<T> {
  protected final Object owner;
  protected final File   file;
  protected final File   errorFile;
  private final   Object lock;
  private final   String watcherName;
  private final   String description;
  // The watcher's baseline. VOLATILE for the timer thread's tick, which compares against it without the lock; written
  // under the lock only. Stamped BEFORE every read, so a file that fails is not re-read on every tick and an edit landing
  // after the stamp is newer and picked up by the next one.
  private volatile long   fileLastUpdated = 0L;
  // VOLATILE for the tick's stale-instance check; written under the lock only. See stop() for why the lock and not this.
  private volatile Timer  watcher;

  /**
   * @param owner       the store this policy belongs to, the requester its log lines are attributed to
   * @param lock        the store's lock, held across every read and publish
   * @param file        the live file
   * @param errorFile   where a file that cannot be used is preserved
   * @param watcherName the prefix of the watcher thread's name; the file's path is appended to it
   * @param description what the watcher's INFO line calls the file, e.g. "Server groups configuration"
   */
  ReloadableSecurityArtifact(final Object owner, final Object lock, final File file, final File errorFile,
      final String watcherName, final String description) {
    this.owner = owner;
    this.lock = lock;
    this.file = file;
    this.errorFile = errorFile;
    this.watcherName = watcherName;
    this.description = description;
  }

  /**
   * Turns the file, which exists, into a value fit to be published. Writes nothing - except, for the groups store, the
   * migration's own save of a valid older document.
   *
   * @return the value, or {@code null} when the file holds no document (the users store's empty file)
   * @throws IOException      if the file cannot be read
   * @throws RuntimeException if it does not parse or is not of the right shape
   */
  protected abstract T read() throws IOException;

  /** What a first load publishes when there is no file, no document in it, or one it cannot use. */
  protected abstract T fallback();

  /**
   * Makes {@code value} the one in force.
   *
   * @param isFallback whether it is a first load's {@link #fallback()} rather than what the file holds
   */
  protected abstract void publish(T value, boolean isFallback);

  /**
   * Whether something is in force, i.e. whether {@link #reload()} has something to keep. When it has not, the reload
   * is a first load. The store decides, because what is in force can also arrive without this class: a replicated
   * install, or a save.
   */
  protected abstract boolean hasInForce();

  /** What a reload that cannot use the file does instead, for its log line: "keeping the ... already in force". */
  protected abstract String keeping();

  /** What a first load that cannot use the file does instead, for its log line. */
  protected String fallingBack() {
    return "using the default configuration";
  }

  /**
   * The value a first load publishes after the file failed with {@code failure}, or {@code null} to publish nothing and
   * leave what is there. The default preserves the file and falls back. A store overrides it only where its first load
   * has a different, pinned, answer:
   * <ul>
   *   <li>the users store refuses to start on a failure that is not a bad file (anything but an I/O error or a
   *       {@code JSONException}), by rethrowing it;</li>
   *   <li>the tokens store keeps what it holds on an I/O error ({@code Issue7373ClusterWideGroupsAndTokensTest}).</li>
   * </ul>
   */
  protected T onFirstLoadFailure(final Exception failure) {
    preserve(failure, fallingBack());
    return fallback();
  }

  /**
   * Whether a reload may read the file at all, checked after {@link #hasInForce()}. {@code false} keeps what is in force
   * and stamps the baseline, so the refusal is logged once per change of the file, not once per tick; the store logs
   * it. The tokens store refuses while the tokens in force have not reached the disk.
   */
  protected boolean mayReload() {
    return true;
  }

  /** Called on the watcher thread after a reload published something; the groups store refreshes permissions here. */
  protected void onReloaded() {
  }

  /**
   * Whether the watcher's tick should reload. The default compares the file's stamp with this class's baseline. The
   * users store overrides it with its repository's stamp, which its own saves advance (unlike the groups and tokens
   * stores, whose saves cost one redundant reload, see {@link SecurityGroupFileRepository#save}).
   */
  protected boolean fileChanged() {
    return file.exists() && file.lastModified() > fileLastUpdated;
  }

  /** Preserves the file as {@link #errorFile} and logs the failure with {@code outcome}. Called under the lock. */
  protected void preserve(final Exception failure, final String outcome) {
    SecurityUserFileRepository.preserveInError(owner, file, errorFile, failure, outcome);
  }

  /**
   * The first load: publishes what the file holds, or {@link #fallback()} when there is no file or no document in it, or
   * whatever {@link #onFirstLoadFailure} answers when the file cannot be used.
   */
  final void firstLoad() {
    synchronized (lock) {
      T value;
      boolean isFallback = false;
      try {
        value = readIfExists();
      } catch (final IOException | RuntimeException e) {
        value = onFirstLoadFailure(e);
        if (value == null)
          return;
        isFallback = true;
      }

      if (value == null) {
        value = fallback();
        isFallback = true;
      }
      publish(value, isFallback);
    }
  }

  /**
   * Re-reads the file on a running server. A file that is missing, holds no document, cannot be read, does not parse or
   * is not of the right shape keeps what is in force: a missing or empty file is logged at WARNING (typically an editor
   * caught between deleting or truncating the file and writing it: the next tick sees the content), any other failure at
   * SEVERE, and a file that could be read is preserved as {@link #errorFile}. Nothing is written over the live file, so
   * the operator can fix it in place and the next tick picks the fix up.
   * <p>
   * With nothing in force there is nothing to keep, and this is the first load.
   *
   * @return {@code true} when a value was published - the file's, or on a first load the fallback - and {@code false}
   *         when what is in force was kept
   */
  final boolean reload() {
    synchronized (lock) {
      if (!hasInForce()) {
        firstLoad();
        return true;
      }

      if (!mayReload()) {
        fileLastUpdated = file.lastModified();
        return false;
      }

      final T value;
      try {
        value = readIfExists();
      } catch (final IOException | RuntimeException e) {
        // ANY failure, not only a parse error: whatever an unexpected shape throws must neither replace what is in force
        // nor escape into the watcher's java.util.Timer, which an uncaught exception kills.
        preserve(e, keeping());
        return false;
      }

      if (value == null) {
        LogManager.instance().log(owner, Level.WARNING, "File '%s' is missing or empty, %s", file.getName(), keeping());
        return false;
      }

      publish(value, false);
      return true;
    }
  }

  private T readIfExists() throws IOException {
    if (!file.exists())
      return null;
    fileLastUpdated = file.lastModified();
    return read();
  }

  /**
   * Schedules the watcher that calls {@link #reload()} when {@link #fileChanged()}. Idempotent, callable before the file
   * exists (the tick checks), and one-shot per instance: {@link #stop()} cancels the timer and leaves the reference set,
   * so a stopped store does not start watching again. That is not a restart hole - {@code ArcadeDBServer} builds a new
   * {@code ServerSecurity}, and with it new stores, on every start.
   * <p>
   * The file's current stamp becomes the baseline when nothing has been read yet: without it the first tick would treat
   * a file nobody touched as changed and reload it on every single start.
   * <p>
   * The timer is a DAEMON, so a caller that skips {@link #stop()} cannot hold a JVM open, and its thread is named after
   * the file: one JVM can run several embedded nodes (every HA test fixture does), and a thread dump must be able to say
   * which node a watcher belongs to.
   *
   * @param checkEveryMs the polling period; nothing is scheduled when it is not positive ({@link Timer#schedule} refuses
   *                     a non-positive period)
   */
  final void startWatching(final int checkEveryMs) {
    synchronized (lock) {
      if (watcher != null || checkEveryMs <= 0)
        return;

      if (fileLastUpdated == 0L && file.exists())
        fileLastUpdated = file.lastModified();

      final Timer timer = new Timer(watcherName + "[" + file.getPath() + "]", true);
      watcher = timer;
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          // A stopped instance's timer may still fire once; a replaced one must not act (this happens during tests).
          if (watcher != timer)
            return;
          try {
            if (fileChanged()) {
              LogManager.instance().log(owner, Level.INFO, "%s changed, reloading it...", description);
              if (reload())
                onReloaded();
            }
          } catch (final Throwable e) {
            LogManager.instance().log(owner, Level.SEVERE, "Error on reloading file '%s' after was changed", e, file.getName());
          }
        }
      }, checkEveryMs, checkEveryMs);
    }
  }

  /**
   * Cancels the watcher. Under the lock, and it has to be: the check and the cancel are one action against
   * {@link #startWatching}, which is check-then-act too. A {@code stop()} that read the field between that method's null
   * check and its assignment would see {@code null}, return without cancelling, and leave the timer created a moment
   * later polling a stopped store for the life of the JVM. {@code volatile} does not close that window; only the lock
   * does. It cannot stall a shutdown: {@link Timer#cancel()} discards the queue without waiting for a running task.
   */
  final void stop() {
    synchronized (lock) {
      if (watcher != null)
        watcher.cancel();
    }
  }
}
