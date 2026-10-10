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
package com.arcadedb.server.http;

import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QuerySession;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * Manage a transaction on the HTTP protocol. Also acts as the {@link QuerySession} that ISO GQL Session
 * Management statements ({@code SESSION SET/RESET/CLOSE}) operate on: it carries session parameters that
 * subsequent commands in the same session see as query parameters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HttpSession implements QuerySession {
  private static final long                DEFAULT_TIMEOUT = 5_000;
  public final         String              id;
  public final         TransactionContext  transaction;
  public final         ServerSecurityUser  user;
  private final        HttpSessionManager  manager;
  // Accessed only under the session lock (see execute()), so a plain HashMap is sufficient.
  private final        Map<String, Object> parameters      = new HashMap<>();
  // Cached live read-only view over 'parameters' (reflects mutations), so getParameters() allocates nothing.
  private final        Map<String, Object> parametersView  = Collections.unmodifiableMap(parameters);
  private final        ReentrantLock       lock            = new ReentrantLock();
  private volatile     long                lastUpdate      = System.currentTimeMillis();
  // Set when a failed command rolled the transaction back (issue #9006): what the client wrote before is gone, so the session
  // refuses every request that would run in it, instead of running them in autocommit and letting /commit report success.
  private volatile     boolean             rolledBackByFailure;
  // The label the client gave the session at /begin (issue #9680): the statements run in it are listed with it, unless a
  // request carries a label of its own, and "terminate transaction" finds the session by it
  private volatile     String              tag;

  public HttpSession(final ServerSecurityUser user, final String id, final TransactionContext dbTx,
      final HttpSessionManager manager) {
    this.user = user;
    this.id = id;
    this.transaction = dbTx;
    this.manager = manager;
  }

  @Override
  public void setParameter(final String name, final Object value) {
    parameters.put(name, value);
  }

  @Override
  public Map<String, Object> getParameters() {
    return parametersView;
  }

  @Override
  public void reset() {
    parameters.clear();
  }

  @Override
  public void close() {
    // Invalidate the session so later references to its id fail, then roll back its open transaction.
    manager.removeSession(id);
    cancel();
  }

  public String getTag() {
    return tag;
  }

  public void setTag(final String tag) {
    this.tag = tag;
  }

  /** Whether a request is running in the session right now. A snapshot: the answer can change as soon as it is read. */
  public boolean isBusy() {
    return lock.isLocked();
  }

  public long elapsedFromLastUpdate() {
    return System.currentTimeMillis() - lastUpdate;
  }

  /**
   * Rolls back the session's transaction. Waits for any in-flight {@link #execute} to finish before doing so.
   * This wait is unbounded on purpose: callers such as {@link #close()} remove the session from
   * {@link HttpSessionManager} tracking first, so a bounded wait that gave up while a command was still
   * in-flight would leak the transaction - nothing else would ever roll it back. Logs a throttled WARNING if
   * the wait exceeds {@link #DEFAULT_TIMEOUT}, so a stuck server shutdown (which calls this per session) is
   * diagnosable instead of a silent hang. Residual gap: if the calling thread is itself interrupted while
   * waiting for the lock, this returns without rolling back and, since {@link #close()} has already
   * untracked the session, nothing retries it - narrower than the bounded-timeout leak this method was
   * written to close, but not eliminated for that one case.
   */
  public boolean cancel() {
    try {
      final long start = System.currentTimeMillis();
      while (!lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS))
        LogManager.instance().log(this, Level.WARNING,
            "Session %s cancel() still waiting for an in-flight command to finish (%dms so far)", id,
            System.currentTimeMillis() - start);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }

    try {
      return rollbackIfActive();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Rolls back the session's transaction only if no command is currently executing on it. Used by the
   * idle-timeout sweep ({@link HttpSessionManager#checkSessionsValidity}), which runs on a background timer
   * thread and must never tear down a transaction while a worker thread is still inside {@link #execute}
   * mutating it - doing so raced {@code TransactionContext.rollback()} against in-flight page mutations and
   * could surface as an NPE on a just-nulled transaction field (issue #4857). If a command is in-flight, this
   * is a no-op; the session is re-evaluated on the sweep's next tick once the command finishes and refreshes
   * {@link #lastUpdate}.
   *
   * @return {@link IdleCancelOutcome#BUSY} if a command is currently in-flight and the sweep must retry
   * later, {@link IdleCancelOutcome#ROLLED_BACK} if idle and an active transaction was rolled back, or
   * {@link IdleCancelOutcome#ALREADY_IDLE} if idle but there was no active transaction to roll back
   */
  IdleCancelOutcome cancelIfIdle() {
    if (!lock.tryLock())
      return IdleCancelOutcome.BUSY;

    try {
      return rollbackIfActive() ? IdleCancelOutcome.ROLLED_BACK : IdleCancelOutcome.ALREADY_IDLE;
    } finally {
      lock.unlock();
    }
  }

  enum IdleCancelOutcome {BUSY, ROLLED_BACK, ALREADY_IDLE}

  private void restartTransaction() {
    try {
      transaction.begin(transaction.getIsolationLevel());
    } catch (final Exception ex) {
      // THE SESSION CANNOT GO ON IN A TRANSACTION: IT IS REFUSED RATHER THAN RUN IN AUTOCOMMIT
      LogManager.instance().log(this, Level.WARNING, "Session %s could not start a new transaction after a failed command", ex, id);
      rolledBackByFailure = true;
    }
  }

  /**
   * Rolls back the transaction if active. Must be called while holding {@code lock}.
   */
  private boolean rollbackIfActive() {
    try {
      if (transaction != null && transaction.isActive()) {
        transaction.rollback();
        return true;
      }
    } catch (Exception e) {
      // IGNORE IT
    }
    return false;
  }

  public HttpSession execute(final ServerSecurityUser user, final Callable callback) throws Exception {
    return execute(user, callback, true);
  }

  /**
   * {@link #execute(ServerSecurityUser, Callable)} with control over what a failure does to the session's
   * transaction (issue #7734).
   * <p>
   * {@code rollbackOnFailure} is true for every request that runs INSIDE the caller's transaction, which is what
   * the single-argument form means and what it has always done: the command failed part way, so the transaction
   * it was running in cannot be trusted and is rolled back before the exception leaves.
   * <p>
   * It is false for a route that must not destroy the transaction when it fails. That is a different question from
   * whether the route runs in it, which the five-argument form's {@code joinsTransaction} answers (issue #9312): the
   * /ws insert session passes false here and still writes into the caller's transaction. It is also false for a route
   * whose documented contract is that it does not participate in that transaction at all
   * - the three {@code /api/v1/ts} routes since issue #7402, which resolve the session for its lock, principal
   * and idle clock but write through {@code TimeSeriesShard.appendSamples}' own nested begin/commit or read
   * without touching it. For those the rollback was pure collateral damage: a 400 from a body the line-protocol
   * parser refused before a single byte reached the engine, or a 413 from a read answering "too many rows",
   * destroyed a transaction the client had opened with {@code /begin} and still believed it owned - and left the
   * session registered, so its later {@code /commit} reported nothing lost.
   * <p>
   * The session lock, the registration re-validation and the idle-clock refresh are identical either way. Only
   * the {@code catch} arm differs, so a handler that declares itself independent cannot accidentally get a
   * WEAKER lock or a stale {@code lastUpdate} out of the choice.
   */
  public HttpSession execute(final ServerSecurityUser user, final Callable callback, final boolean rollbackOnFailure)
      throws Exception {
    return execute(user, callback, rollbackOnFailure, false);
  }

  public HttpSession execute(final ServerSecurityUser user, final Callable callback, final boolean rollbackOnFailure,
      final boolean endsSession) throws Exception {
    return execute(user, callback, rollbackOnFailure, endsSession, rollbackOnFailure);
  }

  /** True when a failed command rolled the transaction back (issue #9006). */
  public boolean isRolledBackByFailure() {
    return rolledBackByFailure;
  }

  /**
   * @param endsSession true for the routes that end the session (/commit, /rollback): they run even when a failed command already
   *                    rolled the transaction back, every other one is refused (issue #9006)
   * @param joinsTransaction true when the callback writes into the session's transaction, whatever it wants done with that
   *                    transaction on failure: such a request is refused once a failed command rolled the transaction back, because
   *                    the work would otherwise autocommit into the gap (issue #9312). Independent of {@code rollbackOnFailure}
   */
  public HttpSession execute(final ServerSecurityUser user, final Callable callback, final boolean rollbackOnFailure,
      final boolean endsSession, final boolean joinsTransaction) throws Exception {
    if (!this.user.equals(user))
      throw new SecurityException("Cannot use the requested transaction because in use by a different user");

    lastUpdate = System.currentTimeMillis();

    if (lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS)) {
      try {
        // RE-VALIDATE UNDER THE LOCK: THE IDLE-TIMEOUT SWEEP (OR A commit/rollback/close) COULD HAVE ROLLED BACK
        // AND REMOVED THIS SESSION IN THE GAP BETWEEN THE MANAGER LOOKUP AND ACQUIRING THIS LOCK. RUNNING THE
        // CALLBACK NOW WOULD OPERATE ON A ROLLED-BACK TRANSACTION AND LEAK A FRESH ONE (NOTHING WOULD EVER
        // COMMIT/ROLL IT BACK, SINCE THE SESSION IS ALREADY UNTRACKED)
        if (!manager.isSessionRegistered(id))
          throw new HttpSessionException("Remote transaction '" + id + "' not found or expired");

        if (rolledBackByFailure && joinsTransaction && !endsSession)
          throw new HttpSessionException("Remote transaction '" + id
              + "' was rolled back after a failed command and cannot run anything more. Send /rollback to end the session");

        LogManager.instance().log(this, Level.FINE, "Executing session %s for user %s", id, user.getName());
        callback.call();
        // REFRESH WHILE STILL HOLDING THE LOCK: OTHERWISE THE IDLE-TIMEOUT SWEEP COULD tryLock() SUCCESSFULLY
        // IN THE GAP BETWEEN UNLOCK AND THIS ASSIGNMENT, SEE A STALE lastUpdate, AND ROLL BACK A TRANSACTION
        // WHOSE COMMAND JUST FINISHED
        lastUpdate = System.currentTimeMillis();
      } catch (Exception e) {
        // ROLLBACK SERVER-SIDE TRANSACTION. CALL rollbackIfActive() DIRECTLY (NOT cancel()): THIS THREAD
        // ALREADY HOLDS `lock` HERE, AND ReentrantLock.lockInterruptibly() CHECKS Thread.interrupted() BEFORE
        // ITS REENTRANT FAST PATH - IF THIS THREAD'S INTERRUPT FLAG IS SET, cancel() WOULD THROW
        // InterruptedException AND SILENTLY SKIP THE ROLLBACK
        if (rollbackOnFailure) {
          final boolean wasActive = transaction != null && transaction.isActive();
          final boolean hadChanges = wasActive && transaction.hasChanges();
          final boolean rolledBack = rollbackIfActive();
          // A TRANSACTION THE ENGINE ALREADY ENDED BENEATH THE COMMAND TOOK ITS WORK WITH IT: NOTHING CAN BE SAID TO BE SAFE
          if (!wasActive || hadChanges)
            // WORK THE CLIENT WROTE BEFORE THE FAILURE IS GONE (OR ITS ROLLBACK FAILED): THE SESSION MUST NOT PRETEND OTHERWISE (issue #9006)
            rolledBackByFailure = true;
          else if (rolledBack)
            // NOTHING WAS LOST (A FAILED READ, AN ERROR BEFORE THE FIRST WRITE): THE SESSION GOES ON IN A FRESH TRANSACTION
            // INSTEAD OF FALLING BACK TO AUTOCOMMIT
            restartTransaction();
        }
        throw e;
      } finally {
        lock.unlock();
      }
    } else {
      throw new LockTimeoutException("Timeout on locking http session");
    }

    return this;
  }
}
