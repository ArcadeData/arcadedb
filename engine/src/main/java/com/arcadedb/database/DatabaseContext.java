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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QuerySession;
import com.arcadedb.security.SecurityDatabaseUser;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Thread local to store transaction data.
 */
public class DatabaseContext extends ThreadLocal<Map<String, DatabaseContext.DatabaseContextTL>> {
  public static final  DatabaseContext                         INSTANCE          = new DatabaseContext();
  // KEYED BY Thread.threadId(): ON JDK 21+ (THIS PROJECT'S BASELINE) threadId() IS SPECIFIED AS A POSITIVE ID
  // GENERATED WHEN THE THREAD IS CREATED AND NEVER REUSED - UNLIKE THE OLD Thread.getId(), WHOSE CONTRACT
  // EXPLICITLY PERMITTED REUSE AFTER TERMINATION. EVERY EXACTLY-ONCE CLAIM/REMOVE ARGUMENT IN THIS CLASS
  // (THE SWEEP'S KEYED CLAIM, removeContext, removeCurrentThreadContexts) DEPENDS ON THIS NON-REUSE:
  // DO NOT REFACTOR TO getId() OR ANY OTHER REUSABLE KEY, AND RE-VERIFY THE GUARANTEE ON JDK MAJOR BUMPS
  private static final ConcurrentHashMap<Long, ThreadContexts> CONTEXTS          = new ConcurrentHashMap<>();
  private static final AtomicInteger                           INIT_CALL_COUNTER = new AtomicInteger();
  private static final int                                     CLEANUP_INTERVAL  = 1000;

  /**
   * One CONTEXTS entry: the per-thread contexts map plus a weak reference to the owning thread, used for the
   * liveness check of the periodic dead-thread sweep. The previous implementation asked
   * {@code Thread.getAllStackTraces()} for the live thread ids, which is wrong for virtual threads (they never
   * appear in it, so a live virtual thread's entry was pruned as dead while it was still running its transaction)
   * and captures the full stack of every platform thread at a global safepoint, injecting a latency spike into
   * whichever request happened to be the 1000th {@code init()} (issue #4956). {@code Thread#isAlive()} on the
   * weak reference is O(1) per entry, safepoint-free and correct for both thread kinds; the weak reference avoids
   * pinning terminated threads (and their stacks) in memory.
   */
  private static final class ThreadContexts {
    private final WeakReference<Thread>          owner;
    private final Map<String, DatabaseContextTL> contexts;

    private ThreadContexts(final Thread owner, final Map<String, DatabaseContextTL> contexts) {
      this.owner = new WeakReference<>(owner);
      this.contexts = contexts;
    }
  }

  public DatabaseContextTL init(final DatabaseInternal database) {
    return init(database, null);
  }

  public DatabaseContextTL init(final DatabaseInternal database, final TransactionContext firstTransaction) {
    // PERIODIC CLEANUP OF DEAD THREAD ENTRIES AS SAFETY NET
    if (INIT_CALL_COUNTER.incrementAndGet() % CLEANUP_INTERVAL == 0)
      cleanupDeadThreads();

    Map<String, DatabaseContextTL> map = get();

    final String key = database.getDatabasePath();

    DatabaseContextTL current;

    if (map == null) {
      // CONCURRENT MAP BECAUSE removeAllContexts() (DATABASE CLOSE/DROP ON ANOTHER THREAD) MUTATES THIS MAP
      // CONCURRENTLY WITH THE OWNER THREAD'S init()/removeContext(); A PLAIN HashMap COULD LOSE ENTRIES OR
      // CORRUPT THE TABLE (ISSUE #4939)
      map = new ConcurrentHashMap<>();
      set(map);
      current = new DatabaseContextTL();
      map.put(key, current);
    } else {
      current = map.get(key);
      if (current == null) {
        current = new DatabaseContextTL();
        map.put(key, current);
      } else {
        if (!current.transactions.isEmpty()) {
          // ROLLBACK PREVIOUS TXS
          while (!current.transactions.isEmpty()) {
            final Transaction tx = current.transactions.remove(current.transactions.size() - 1);
            try {
              tx.rollback();
            } catch (final Exception e) {
              // IGNORE ANY ERROR DURING ROLLBACK
            }
          }
        }
      }
    }

    // ALWAYS ENSURE THE MAP IS REGISTERED IN CONTEXTS (the dead-thread sweep may have pruned this entry).
    // AVOID THE PUT (AND THE WeakReference ALLOCATION) WHEN THE SAME MAP IS ALREADY REGISTERED: THE MAP IS
    // PER-THREAD, SO IDENTITY IMPLIES THE ENTRY BELONGS TO THIS THREAD
    final Thread currentThread = Thread.currentThread();
    final ThreadContexts registered = CONTEXTS.get(currentThread.threadId());
    if (registered == null || registered.contexts != map)
      CONTEXTS.put(currentThread.threadId(), new ThreadContexts(currentThread, map));

    if (current.transactions.isEmpty())
      current.transactions.add(
          firstTransaction != null ? firstTransaction : new TransactionContext(database.getWrappedDatabaseInstance()));

    return current;
  }

  public DatabaseContextTL getContext(final String name) {
    final DatabaseContextTL ctx = getContextIfExists(name);
    if (ctx == null)
      throw new DatabaseOperationException("Transaction context not found on current thread");
    return ctx;
  }

  public DatabaseContextTL getContextIfExists(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    return map != null ? map.get(name) : null;
  }

  public DatabaseContextTL removeContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    if (map != null) {
      final DatabaseContextTL tl = map.remove(name);
      if (map.isEmpty()) {
        // REMOVE THE THREAD LOCAL WHEN THE MAP IS EMPTY. THE UNCONDITIONAL (NON-VALUE-KEYED) REMOVE IS SAFE,
        // UNLIKE IN THE SWEEP: THIS RUNS ON THE OWNER THREAD, ONLY THE OWNER EVER REGISTERS UNDER ITS OWN
        // NEVER-REUSED THREAD ID (SEE THE threadId() PROVENANCE NOTE ON CONTEXTS) AND THE SWEEP ONLY CLAIMS
        // DEAD THREADS, SO THE ENTRY UNDER THIS KEY CAN ONLY BE THIS THREAD'S OWN CURRENT REGISTRATION
        super.remove();
        CONTEXTS.remove(Thread.currentThread().threadId());
      }
      return tl;
    }
    return null;
  }

  /**
   * Remove the database instance from all the thread local contexts.
   */
  public List<DatabaseContextTL> removeAllContexts(final String databaseName) {
    final List<DatabaseContextTL> result = new ArrayList<>();
    for (final ThreadContexts threadContexts : CONTEXTS.values()) {
      final DatabaseContextTL tl = threadContexts.contexts.remove(databaseName);
      if (tl != null)
        result.add(tl);
      // DO NOT PRUNE THE CONTEXTS ENTRY HERE EVEN WHEN THE MAP BECAME EMPTY: THIS RUNS ON THE CLOSING THREAD AND
      // WOULD RACE THE OWNER THREAD'S init() RE-REGISTRATION, UNREGISTERING A LIVE CONTEXT (ISSUE #4939). AN
      // EMPTY ENTRY LINGERS UNTIL ITS OWNER NEXT TOUCHES THE CONTEXT API (removeContext/
      // removeCurrentThreadContexts), DIES (DEAD-THREAD SWEEP) OR THE PERIODIC SWEEP OPPORTUNISTICALLY DROPS
      // IT WITH ITS CLAIM/RE-CHECK PROTOCOL (#5067): AN IDLE LIVE THREAD KEEPS A TINY EMPTY-MAP ENTRY
      // MEANWHILE, BOUNDED BY THE LIVE-THREAD COUNT
    }
    return result;
  }

  /**
   * This method is used by Gremlin for IO only. Using the TL to retrieve the current database is not recommended.
   */
  public Database getActiveDatabase() {
    final Map<String, DatabaseContextTL> map = get();
    if (map == null || map.isEmpty())
      return null;

    if (map.size() == 1) {
      final DatabaseContextTL tl = map.values().iterator().next();
      if (tl != null) {
        final TransactionContext tx = tl.getLastTransaction();
        if (tx != null)
          return tx.getDatabase();
      }
      return null;
    }

    // MULTIPLE DATABASES: RETURN THE ONE WITH AN ACTIVE TRANSACTION
    Database candidate = null;
    for (final DatabaseContextTL tl : map.values()) {
      if (tl != null) {
        final TransactionContext tx = tl.getLastTransaction();
        if (tx != null && tx.isActive()) {
          if (candidate != null)
            return null; // AMBIGUOUS: MULTIPLE ACTIVE TRANSACTIONS
          candidate = tx.getDatabase();
        }
      }
    }

    if (candidate != null)
      return candidate;

    // FALLBACK: IF NO ACTIVE TRANSACTION, RETURN ANY DATABASE THAT HAS A TRANSACTION CONTEXT
    for (final DatabaseContextTL tl : map.values()) {
      if (tl != null) {
        final TransactionContext tx = tl.getLastTransaction();
        if (tx != null)
          return tx.getDatabase();
      }
    }
    return null;
  }

  /**
   * Removes all database contexts for the current thread. Virtual thread callers should invoke this in their finally blocks
   * to prevent memory leaks since virtual threads may not be garbage collected promptly while referenced by CONTEXTS.
   */
  public void removeCurrentThreadContexts() {
    super.remove();
    // UNCONDITIONAL REMOVE IS SAFE FOR THE SAME REASON AS IN removeContext(): OWNER-THREAD-ONLY KEY,
    // THREAD IDS NEVER REUSED (SEE THE threadId() PROVENANCE NOTE ON CONTEXTS), THE SWEEP ONLY CLAIMS
    // DEAD THREADS
    CONTEXTS.remove(Thread.currentThread().threadId());
  }

  /**
   * Scans CONTEXTS and removes the entries whose owning thread is no longer alive, rolling back any transaction
   * they abandoned. Also opportunistically drops the empty entries of LIVE threads left behind by a foreign
   * database close ({@code removeAllContexts} cannot prune them without racing the owner, see #4939/#5067);
   * that prune is allocation-free and never rolls anything back. Called periodically as a safety net. Liveness is checked per entry via the thread weak
   * reference ({@link ThreadContexts}), which is virtual-thread-correct and safepoint-free (issue #4956).
   * Rolling back the abandoned transactions releases their file locks: before this, the sweep dropped the map
   * entries only, so the {@code LockManager} files locked by a thread that died with an open transaction (for
   * example via {@code acquireLock().type(...).lock()}) stayed owned by the dead thread forever and every later
   * commit touching them timed out until restart (issue #4941).
   * <p>
   * The rollbacks run INLINE on the triggering thread: unlike the pre-#4941 sweep (a cheap map cleanup), the
   * unlucky request whose {@code init()} hits the periodic boundary pays for the full rollback (callbacks, page
   * reloads, lock release) of every transaction the dead threads abandoned. Acceptable for a safety-net path,
   * but operators should expect a tail-latency blip when the sweep finds abandoned work; a sweep that performs
   * real rollback work emits a single WARNING summarizing count, thread ids and databases so the blip is
   * attributable (zero-work sweeps stay silent). The rollback may also
   * re-enter this class on the sweeping thread (record reloads resolve the sweeper's own transaction context);
   * that is safe because the dead entry is atomically claimed and unlinked before the rollbacks start and the
   * CONTEXTS traversal is weakly consistent.
   * A rollback can also re-enter init() and, on the 1000th call, trigger a NESTED full sweep - safe for the
   * same reasons (weakly-consistent traversal + the atomic claim: a nested sweep cannot double-roll-back a
   * claimed entry), noted so the recursion is a documented possibility rather than a surprise.
   * <p>
   * Offloading the rollbacks to {@code DatabaseAsyncExecutor} was considered and rejected: that executor is
   * PER-DATABASE and may itself be closing or saturated exactly when the sweep runs (the sweep reclaims
   * contexts for ANY database, including ones mid-close), and a rollback queued behind a stalled async queue
   * delays the lock release this sweep exists to guarantee. Revisit only with a dedicated JVM-wide cleanup
   * executor if the inline cost ever shows up in practice.
   * <p>
   * Package-private (instead of private) for tests only.
   */
  static void cleanupDeadThreads() {
    int rolledBack = 0;
    StringBuilder details = null;

    for (final Map.Entry<Long, ThreadContexts> entry : CONTEXTS.entrySet()) {
      final ThreadContexts threadContexts = entry.getValue();
      final Thread owner = threadContexts.owner.get();
      if (owner == null || !owner.isAlive()) {
        // MEMORY VISIBILITY: THE isAlive() == false CHECK IS LOAD-BEARING, NOT JUST A LIVENESS TEST. PER JLS
        // 17.4.4 THE FINAL ACTION OF A TERMINATED THREAD SYNCHRONIZES-WITH ANOTHER THREAD DETECTING THE
        // TERMINATION VIA isAlive()/join(), SO THIS THREAD IS GUARANTEED TO SEE ALL THE DEAD OWNER'S WRITES
        // EVEN THE NON-VOLATILE ONES (TransactionContext.requester IS VOLATILE FOR THE
        // LIVE-OWNER CLOSE PATH, BUT THE DEAD-OWNER GUARANTEE HERE DOES NOT DEPEND ON THAT). DO NOT REPLACE
        // THE LIVENESS TEST WITH A BARE
        // WeakReference STALENESS CHECK. THE owner == null BRANCH LACKS THE FORMAL GUARANTEE BUT A GC-CLEARED
        // Thread IS LONG TERMINATED (AND ITS WRITES LONG PUBLISHED) IN PRACTICE.
        //
        // ATOMICALLY CLAIM THE DEAD ENTRY: TWO THREADS CAN SWEEP CONCURRENTLY (BOTH LANDING ON THE PERIODIC
        // init() BOUNDARY) AND THE ABANDONED TRANSACTIONS MUST BE ROLLED BACK EXACTLY ONCE. THREAD IDS ARE
        // NEVER REUSED (SEE THE threadId() PROVENANCE NOTE ON THE CONTEXTS FIELD), SO THE KEYED REMOVE
        // CANNOT DROP AN ENTRY BELONGING TO A DIFFERENT THREAD
        if (CONTEXTS.remove(entry.getKey(), threadContexts))
          for (final Map.Entry<String, DatabaseContextTL> ctx : threadContexts.contexts.entrySet())
            // CLAIM EACH PER-DATABASE CONTEXT TOO: closeInternal (DATABASE CLOSE/DROP) CONCURRENTLY CLAIMS
            // THE SAME tl VIA removeAllContexts()'s remove(databaseName) AND ROLLS IT BACK UNDER THE DB
            // WRITE LOCK, WHICH THE SWEEP DOES NOT TAKE. THE VALUE-KEYED REMOVE GUARANTEES EXACTLY ONE OF
            // THE TWO PATHS ROLLS BACK EACH ABANDONED TRANSACTION - SAME HAZARD CLASS AS THE
            // SWEEPER-VS-SWEEPER CLAIM ABOVE (DOUBLE UNLOCK, CONCURRENT MUTATION OF tl.transactions)
            if (threadContexts.contexts.remove(ctx.getKey(), ctx.getValue())) {
              final int rolled = rollbackAbandonedTransactions(ctx.getValue(), entry.getKey(), ctx.getKey());
              if (rolled > 0) {
                rolledBack += rolled;
                if (details == null)
                  details = new StringBuilder();
                else
                  details.append(", ");
                details.append("threadId=").append(entry.getKey()).append(" database='").append(ctx.getKey())
                    .append("' transactions=").append(rolled);
              }
            }
      } else if (threadContexts.contexts.isEmpty()) {
        // #5067: A LIVE THREAD'S ENTRY WHOSE PER-DATABASE MAP IS NOW EMPTY, LEFT BEHIND BY A FOREIGN CLOSE
        // (removeAllContexts DELIBERATELY DOES NOT PRUNE, SEE THE #4939 NOTE THERE). DROP IT SO OPEN/CLOSE
        // CHURN ON LARGE LONG-LIVED POOLS (E.G. THE ~500 UNDERTOW WORKERS) DOES NOT ACCUMULATE EMPTY
        // ENTRIES. THE HAZARD IS THE #4939 RACE: THE OWNER'S init() MAY HAVE JUST REPOPULATED THE MAP AND
        // SKIPPED ITS RE-PUT ON THE IDENTITY FAST-PATH, SO A BARE REMOVE COULD UNREGISTER A LIVE CONTEXT.
        // THE CLAIM/RE-CHECK PROTOCOL CLOSES IT: CLAIM THE EXACT ENTRY (VALUE-KEYED REMOVE), RE-CHECK
        // EMPTINESS, AND RESTORE THE SAME ENTRY IF THE OWNER REPOPULATED MEANWHILE. ORDERING ARGUMENT: IN
        // init() THE OWNER'S map.put ALWAYS PRECEDES ITS CONTEXTS REGISTRATION CHECK. IF THAT map.put
        // LINEARIZES BEFORE THE RE-CHECK, THE RE-CHECK SEES NON-EMPTY AND RESTORES (putIfAbsent, SO A
        // REGISTRATION THE OWNER ALREADY REFRESHED WINS); IF AFTER, THE OWNER'S SUBSEQUENT CHECK SEES THE
        // ENTRY GONE AND RE-REGISTERS ITSELF. EITHER WAY NO LIVE REGISTRATION IS LOST. NO ROLLBACKS RUN
        // HERE (THE MAP IS EMPTY), SO THE TWO-LEVEL CLAIM DISCIPLINE ABOVE IS UNTOUCHED; ALLOCATION-FREE
        if (CONTEXTS.remove(entry.getKey(), threadContexts) && !threadContexts.contexts.isEmpty())
          CONTEXTS.putIfAbsent(entry.getKey(), threadContexts);
      }
    }

    // OPERABILITY (#5060 REVIEW): ONE SUMMARY LINE PER SWEEP THAT DID REAL WORK, SO THE INLINE ROLLBACK'S
    // TAIL-LATENCY BLIP ON THE TRIGGERING THREAD IS ATTRIBUTABLE INSTEAD OF MYSTERIOUS. ZERO-WORK SWEEPS
    // (THE OVERWHELMINGLY COMMON CASE) STAY SILENT AND ALLOCATE NOTHING
    if (rolledBack > 0)
      LogManager.instance().log(DatabaseContext.class, Level.WARNING,
          "Dead-thread sweep rolled back %d abandoned transaction(s) inline on thread '%s': %s", rolledBack,
          Thread.currentThread().getName(), details);
  }

  /**
   * Rolls back, from last to first, the transactions abandoned by a dead thread. The rollback is executed on the
   * sweeping thread but releases the file locks with the requester captured at lock-acquisition time (see
   * {@code TransactionContext#getRequester()}), which the {@code LockManager} explicitly supports. Nobody else can
   * touch these transactions: the owner is dead and the caller atomically claimed the tl at BOTH levels (the
   * CONTEXTS entry against concurrent sweepers, the per-database key against closeInternal's
   * removeAllContexts()), so the cross-thread access is exclusive. Returns the number of ACTIVE transactions
   * rolled back, feeding the sweep's operability summary log.
   */
  private static int rollbackAbandonedTransactions(final DatabaseContextTL tl, final long threadId, final String databaseName) {
    int rolledBack = 0;
    for (int i = tl.transactions.size() - 1; i > -1; --i) {
      try {
        final TransactionContext tx = tl.transactions.get(i);
        if (tx.isActive()) {
          tx.rollback();
          ++rolledBack;
        }
      } catch (final Throwable e) {
        // BEST-EFFORT CLEANUP, BUT NEVER SILENT (#5060): A FAILED ROLLBACK MEANS THE ABANDONED
        // TRANSACTION'S FILE LOCKS MAY STILL BE HELD - THE EXACT LEAK THIS SWEEP EXISTS TO FIX - SO THE
        // OPERATOR MUST SEE IT. THE SWEEP CONTINUES WITH THE REMAINING TRANSACTIONS EITHER WAY.
        LogManager.instance().log(DatabaseContext.class, Level.WARNING,
            "Dead-thread sweep failed to roll back an abandoned transaction (threadId=%d, database='%s'): its file locks may remain held",
            e, threadId, databaseName);
      }
    }
    tl.transactions.clear();
    return rolledBack;
  }

  /**
   * Package-private test hook: returns true when the given thread id has a registered context entry.
   */
  static boolean isThreadRegistered(final long threadId) {
    return CONTEXTS.containsKey(threadId);
  }

  /**
   * Package-private test hook: returns true when any registered entry is owned by a still-reachable thread
   * whose name starts with the given prefix. Lets tests key assertions to specific worker threads (e.g. the
   * GAV "gav-worker-" virtual threads) instead of diffing JVM-wide id snapshots, which would go spuriously
   * red whenever an unrelated thread registered a context between the snapshots.
   */
  static boolean isThreadRegisteredWithNamePrefix(final String prefix) {
    for (final ThreadContexts threadContexts : CONTEXTS.values()) {
      final Thread owner = threadContexts.owner.get();
      if (owner != null && owner.getName().startsWith(prefix))
        return true;
    }
    return false;
  }

  public static class DatabaseContextTL {
    public final List<TransactionContext> transactions = new ArrayList<>(3);
    public       boolean                  asyncMode    = false;
    private      Binary                   temporaryBuffer1;
    private      Binary                   temporaryBuffer2;
    private      int                      maxNested    = 3;
    private      SecurityDatabaseUser     currentUser  = null;
    // The stateful client session bound to this thread context (GQL SESSION statements).
    // Set by the protocol owner (HTTP/Bolt) alongside the transaction; null in plain embedded use.
    private      QuerySession             querySession = null;

    public SecurityDatabaseUser getCurrentUser() {
      return currentUser;
    }

    public void setCurrentUser(final SecurityDatabaseUser currentUser) {
      this.currentUser = currentUser;
    }

    public QuerySession getQuerySession() {
      return querySession;
    }

    public void setQuerySession(final QuerySession querySession) {
      this.querySession = querySession;
    }

    public Binary getTemporaryBuffer1() {
      if (temporaryBuffer1 == null) {
        temporaryBuffer1 = new Binary(8192, true);
        temporaryBuffer1.setAllocationChunkSize(1024);
      }
      temporaryBuffer1.clear();
      return temporaryBuffer1;
    }

    public Binary getTemporaryBuffer2() {
      if (temporaryBuffer2 == null) {
        temporaryBuffer2 = new Binary(8192, true);
        temporaryBuffer2.setAllocationChunkSize(1024);
      }
      temporaryBuffer2.clear();
      return temporaryBuffer2;
    }

    public TransactionContext getLastTransaction() {
      if (transactions.isEmpty())
        return null;
      return transactions.getLast();
    }

    public void pushTransaction(final TransactionContext tx) {
      if (transactions.size() + 1 > maxNested)
        throw new TransactionException("Exceeded number of " + transactions.size()
            + " nested transactions. Check your code if you are beginning new transactions without closing the previous one by mistake. Otherwise change this limit with setMaxNested()");

      transactions.add(tx);
    }

    public TransactionContext popIfNotLastTransaction() {
      if (transactions.isEmpty())
        return null;

      if (transactions.size() > 1)
        return transactions.removeLast();

      return transactions.getFirst();
    }

    public int getMaxNested() {
      return maxNested;
    }

    public void setMaxNested(final int maxNested) {
      this.maxNested = maxNested;
    }
  }

}
