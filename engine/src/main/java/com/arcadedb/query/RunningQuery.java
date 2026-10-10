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
package com.arcadedb.query;

import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.serializer.json.JSONObject;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * One statement in progress, as {@link RunningQueryRegistry} lists it, and the switch that stops it (issue #9680).
 * <p>
 * <b>How a termination reaches the work.</b> Cooperatively, through a flag: {@link #terminate(String)} sets it and every
 * {@link com.arcadedb.query.sql.executor.WorkGuard} of the statement reads it, as does the first phase of every commit
 * the statement attempts. The statement then fails with a {@link QueryTerminatedException} and its transaction rolls
 * back on the way out like after any other failure. {@link Thread#interrupt()} is never an option: an interrupted
 * thread that is reading or writing a {@code FileChannel} closes it, and the channel is the database file every other
 * thread reads too.
 * <p>
 * <b>How the work finds its entry.</b> Opening an entry publishes it on the calling thread ({@link #current()}), and the
 * root command context of every statement run on that thread picks it up; derived contexts inherit it from their
 * parent and parallel workers from the context they are copied from, exactly as they share the command deadline. The
 * entry is closed on the thread that opened it, in a {@code finally} block: request threads are pooled.
 * <p>
 * <b>What the entry proves.</b> It is removed from the registry only once the work is over, so its absence from the
 * list is the evidence that the server stopped working on it. {@link #awaitEnd(long)} waits for that moment, and
 * {@link #getOutcome()} then says whether it was the termination that ended the statement or the statement ended on
 * its own first - a write that committed before it reached a check is not undone by a termination that came too late,
 * and the caller is told so rather than told it was stopped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RunningQuery implements AutoCloseable {
  /** Longest statement text kept for the listing: the text is for recognizing the statement, not for re-running it. */
  public static final int MAX_TEXT_LENGTH = 1024;
  /** How far past {@link #MAX_TEXT_LENGTH} the masking looks, so a credential cut by the limit is still recognized. */
  private static final int MASKING_MARGIN = 256;

  /** What the statement's end, once reached, owes to a termination. */
  public enum Outcome {
    /** Still running. */
    RUNNING,
    /** Ended without being asked to stop. */
    COMPLETED,
    /** Asked to stop, but ended on its own before any check saw the request: what it did stands. */
    COMPLETED_BEFORE_TERMINATION,
    /** Stopped by the termination: it failed, or its transaction session was ended, and what it wrote was rolled back. */
    TERMINATED
  }

  private static final ThreadLocal<RunningQuery> CURRENT    = new ThreadLocal<>();
  // The keyword, the closing quote of a JSON key, a separator and an authentication scheme (Authorization: Bearer x),
  // then the value: quoted, or up to the next delimiter. The
  // separator's quantifiers are possessive, so a long run of blanks after a keyword is scanned once, never backtracked
  private static final Pattern                   CREDENTIAL =
      Pattern.compile("(?i)\\b(identified\\s+by|password|passwd|pwd|token|secret|api[_-]?key|authorization|bearer)\\b([\"']?+\\s*+(?:[=:]\\s*+)?(?:(?:bearer|basic)\\s++)?)('[^']*'|\"[^\"]*\"|[^\\s,;)}\\]]+)");

  private final RunningQueryRegistry registry;
  private final long                 id;
  private final String               externalId;
  private final String               database;
  private final String               user;
  private final String               protocol;
  private final String               sessionId;
  private final String               tag;
  private final long                 startedAt;
  private final long                 startedNanos;
  private       RunningQuery         previous;
  private final CountDownLatch       ended = new CountDownLatch(1);
  private volatile String            language;
  private volatile String            text;
  private volatile String            terminatedBy;
  /** Whether a check saw the termination and failed the statement for it. */
  private volatile boolean           terminationObserved;
  /** Whether what the statement wrote was rolled back after it ended, with the transaction session it ran in. */
  private volatile boolean           rolledBack;

  RunningQuery(final RunningQueryRegistry registry, final long id, final String database, final String user,
      final String protocol, final String sessionId, final String tag) {
    this.registry = registry;
    this.id = id;
    this.externalId = "q" + id + "-" + registry.getInstanceTag();
    this.database = database;
    this.user = user;
    this.protocol = protocol;
    this.sessionId = sessionId;
    this.tag = tag;
    this.startedAt = System.currentTimeMillis();
    this.startedNanos = System.nanoTime();
  }

  /**
   * Publishes the entry on the calling thread. Called by {@link RunningQueryRegistry#register} once the entry is in the
   * registry and nothing can fail any more, so an entry that never made it there cannot be left behind on a pooled thread,
   * where the next request would obey its termination.
   */
  void publish() {
    previous = CURRENT.get();
    CURRENT.set(this);
  }

  /** The entry of the statement running on this thread, or {@code null} when nothing registered one. */
  public static RunningQuery current() {
    final RunningQuery query = CURRENT.get();
    // An entry that has ended no longer stands for anything running on the thread, whatever order entries were closed in
    return query != null && query.ended.getCount() > 0 ? query : null;
  }

  /**
   * Records what the statement is, once the protocol has parsed it out of the request. Kept as it came: it is masked and
   * truncated only when it is listed ({@link #getText()}), so a request pays nothing for a listing that seldom happens, and
   * the masking sees the whole text rather than one cut in the middle of a credential.
   */
  public void setStatement(final String language, final String text) {
    this.language = language;
    this.text = text;
  }

  /**
   * The text with the value after {@code IDENTIFIED BY}, {@code PASSWORD} (also {@code PASSWD}, {@code PWD}), {@code TOKEN},
   * {@code SECRET}, {@code API_KEY}
   * (also {@code APIKEY}, {@code API-KEY}), {@code AUTHORIZATION} or {@code BEARER} replaced by {@code ***}, in
   * statement form ({@code IDENTIFIED BY x}, {@code password = 'x'}) as in JSON form ({@code "password": "x"}), then cut to
   * {@link #MAX_TEXT_LENGTH}. The listing is read by the server administrator for every user's statements, and a credential
   * written in clear in a statement is not something it needs to show.
   * <p>
   * Best effort: it recognizes those keywords, not every way a secret can be spelled in a statement. Parameters, the
   * proper place for one, are never listed at all.
   */
  static String maskAndTruncate(final String text) {
    // Only what can be shown is scanned, plus a margin for a credential that straddles the cut: the listing costs the same
    // for a statement of a kilobyte and for one of a hundred megabytes
    final String scanned = text.length() > MAX_TEXT_LENGTH + MASKING_MARGIN ? text.substring(0, MAX_TEXT_LENGTH + MASKING_MARGIN) : text;
    final String masked = CREDENTIAL.matcher(scanned).replaceAll("$1$2***");
    return masked.length() > MAX_TEXT_LENGTH ? masked.substring(0, MAX_TEXT_LENGTH) + "..." : masked;
  }

  /**
   * Asks the statement to stop. The first request wins and names who made it; the statement stops at its next check.
   *
   * @return {@code false} if the statement had already been asked to stop
   */
  public synchronized boolean terminate(final String by) {
    if (terminatedBy != null)
      return false;
    terminatedBy = by != null ? by : "unknown";
    return true;
  }

  public boolean isTerminated() {
    return terminatedBy != null;
  }

  /** Fails the statement if it was asked to stop. {@code what} names the work that noticed, for the message. */
  public void checkNotTerminated(final String what) {
    if (terminatedBy != null && ended.getCount() > 0) {
      terminationObserved = true;
      throw new QueryTerminatedException(
          (what != null ? what : "the command") + " has been terminated (query " + getId() + ", terminated by " + terminatedBy
              + ")");
    }
  }

  /**
   * Waits up to {@code timeoutMs} for the statement to end.
   *
   * @return whether it has ended
   */
  public boolean awaitEnd(final long timeoutMs) throws InterruptedException {
    return timeoutMs <= 0 ? ended.getCount() == 0 : ended.await(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public boolean isEnded() {
    return ended.getCount() == 0;
  }

  public Outcome getOutcome() {
    if (!isEnded())
      return Outcome.RUNNING;
    if (terminatedBy == null)
      return Outcome.COMPLETED;
    return terminationObserved || rolledBack ? Outcome.TERMINATED : Outcome.COMPLETED_BEFORE_TERMINATION;
  }

  /**
   * Records that what the statement wrote was rolled back after it ended - its transaction session was ended because it
   * had been terminated - so it counts as terminated even if it finished before any check saw the request.
   */
  public void setRolledBack() {
    rolledBack = true;
  }

  /**
   * Ends the entry: removes it from the registry and gives the thread back the entry it had before. Must run on the
   * thread that opened the entry, and entries opened on one thread close in the reverse order they were opened (a
   * try-with-resources nesting does that). An entry closed out of turn is not left behind: an ended entry is never what
   * {@link #current()} answers, and closing the inner entry later restores the closest enclosing one still running.
   */
  @Override
  public void close() {
    if (ended.getCount() == 0)
      return;
    try {
      registry.unregister(this);
    } finally {
      ended.countDown();
      if (CURRENT.get() == this) {
        // Back to the closest enclosing entry still running: one closed out of turn is skipped, never restored
        RunningQuery restore = previous;
        while (restore != null && restore.isEnded())
          restore = restore.previous;
        if (restore != null)
          CURRENT.set(restore);
        else
          CURRENT.remove();
      }
    }
  }

  /**
   * The id clients list and terminate it by: {@code q<n>-<instance>}, where the instance tag tells this server's registry
   * from any other's, so a terminate that a load balancer routes to another node finds nothing there instead of stopping
   * that node's statement number {@code n}.
   */
  public String getId() {
    return externalId;
  }

  long getNumericId() {
    return id;
  }

  public String getDatabase() {
    return database;
  }

  public String getUser() {
    return user;
  }

  public String getProtocol() {
    return protocol;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getTag() {
    return tag;
  }

  public String getLanguage() {
    return language;
  }

  /** The statement as it is listed: masked and truncated, see {@link #maskAndTruncate(String)}. */
  public String getText() {
    final String raw = text;
    return raw == null ? null : maskAndTruncate(raw);
  }

  public String getTerminatedBy() {
    return terminatedBy;
  }

  public long getStartedAt() {
    return startedAt;
  }

  public long getElapsedMillis() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject()//
        .put("id", getId())//
        .put("database", database)//
        .put("user", user)//
        .put("protocol", protocol)//
        .put("language", language)//
        .put("text", getText())//
        .put("startedAt", startedAt)//
        .put("elapsedMs", getElapsedMillis())//
        .put("terminating", terminatedBy != null);
    if (sessionId != null)
      json.put("sessionId", sessionId);
    if (tag != null)
      json.put("tag", tag);
    if (terminatedBy != null)
      json.put("terminatedBy", terminatedBy);
    return json;
  }

  @Override
  public String toString() {
    return getId() + " " + language + " '" + getText() + "'";
  }
}
