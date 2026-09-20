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

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Property;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Holds the existence constraints ({@code MANDATORY}, {@code NOTNULL}) of the records a statement is still in the
 * middle of writing, so that they are enforced once the statement has finished rather than at the instant the
 * record first reaches the bucket.
 * <p>
 * The motivating shape is issue #7945, and it is the standard openCypher upsert:
 * <pre>
 *   CREATE CONSTRAINT rec_org_exists IF NOT EXISTS FOR (n:Record) REQUIRE n.orgId IS NOT NULL;
 *   MERGE (n:Record {id: $id}) SET n.orgId = $orgId RETURN n;
 * </pre>
 * {@code MERGE} creates the node from the pattern alone - {@code id} and nothing else - and the {@code SET} that
 * supplies {@code orgId} is a separate step of the same statement, which runs right after. Validating the record
 * at creation asks a question the statement has not finished answering, so the write failed outright and the
 * constraint became unusable with the one write pattern it exists to protect. Neo4j, the openCypher reference
 * implementation, checks existence constraints once the write has fully applied, and so do we now.
 * <p>
 * A record created inside an open scope whose existence constraints are not yet met is <i>provisional</i>: the
 * creation goes through, the record is registered here, and further updates to it inside the same scope skip the
 * same checks - {@code SET n.name = 'x' SET n.orgId = 'y'} must not fail on the first of the two. Nothing else is
 * relaxed: every other constraint (type, min/max, regexp, readonly) is still enforced at the instant of the write,
 * where the value that breaks it is in hand and the error can name it, and a record this scope never created is
 * validated exactly as before.
 * <p>
 * When the statement ends, {@link #check()} re-reads every record still registered. One that has since been
 * completed is simply forgotten. One that has not is <b>deleted</b> and the statement fails: the creation was
 * provisional, so taking it back is what the eager validation this replaces used to achieve by refusing it up
 * front. Leaving it would be worse than the bug being fixed - a record that violates its own type's constraints
 * cannot be updated afterwards either, so it would be stuck in the database with no way to repair it through the
 * normal write paths.
 * <p>
 * What this costs, stated plainly: a provisional record is committed by the write step that created it - the
 * openCypher pipeline auto-commits per step - so between that commit and the end of the statement a concurrent
 * reader can observe a record that does not satisfy its own type's existence constraints, which eager validation
 * made structurally impossible. And because the bookkeeping is this in-memory scope and nothing else, a crash or a
 * killed connection between the creation and {@link #check()} leaves the provisional record behind for good, with
 * no durable trace that would let anything find it later (issue #7952 covers giving operators a way to find one). Both follow from the per-step auto-commit model rather
 * than from the deferral itself - that model already leaves the earlier clauses of a failed statement committed -
 * but the deferral widens the window from "a valid record" to "a record that is not valid yet".
 * <p>
 * Scopes nest: only the outermost {@link #open} returns a scope, so a nested plan (a {@code CALL} subquery, a
 * {@code FOREACH} body, a UNION branch) contributes its provisional records to the enclosing statement's scope and
 * never checks or closes it. That nesting is thread-bound, being keyed on {@link DatabaseContext}: it holds because
 * a nested plan runs on the thread of the statement that drove it, as every openCypher plan does today. A nested
 * plan moved onto another thread would find no scope and validate eagerly - the pre-#7945 behaviour, not a
 * corruption - so this is a load-bearing assumption worth re-checking before any of these steps is parallelised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DeferredExistenceChecks {
  /**
   * Above this many pending records the next registration first re-reads what is pending and forgets whatever has
   * been completed in the meantime. Without it a bulk upsert - {@code UNWIND $rows AS r CREATE (n:R {id: r.id}) SET
   * n.orgId = r.org} - would accumulate one entry per row for the whole statement, even though each row's record is
   * completed by the {@code SET} a few steps later. With it the pending set tracks the write pipeline's own window
   * instead of the statement's total row count.
   */
  private static final int SWEEP_THRESHOLD = 1_000;

  /**
   * How many scopes across the whole JVM are holding at least one provisional record right now.
   * <p>
   * The ordinary write path asks this - one volatile read - before looking anything up, and it counts ARMED scopes
   * rather than open ones on purpose: a scope is open for the length of every openCypher write statement, which on a
   * busy server is always, while it is armed only between a creation that could not be validated yet and the write
   * that completes it. So a database that never leaves a record incomplete pays a single read for this class no
   * matter how much Cypher is running against it.
   * <p>
   * JVM-wide rather than per database, deliberately: one counter is one volatile read, where a per-database one
   * would be a lookup before the read it is meant to avoid. The cost of that choice is that a single database
   * holding a provisional record makes every other database's writes attempt the {@link #completed} lookup too -
   * a thread-local get that finds nothing.
   */
  private static final AtomicInteger ARMED_SCOPES = new AtomicInteger();

  private final DatabaseInternal database;

  /**
   * Records registered before their RID was assigned: {@link LocalDatabase#createRecordNoLock} validates the record
   * before it has an identity, so the instance is what there is to hold on to at that point. Resolved into
   * {@link #pending} at the next registration, by which time the creation that triggered it has returned.
   */
  private final List<MutableDocument> unresolved = new ArrayList<>();

  /** Provisional records by RID, in creation order so the failure reports them in the order they were written. */
  private final Set<RID> pending = new LinkedHashSet<>();

  /**
   * The size {@link #pending} has to reach before the next {@link #sweep()}. It starts at {@link #SWEEP_THRESHOLD}
   * and is set to twice whatever survives a sweep, so a statement whose records never complete - the "forgot the
   * SET" mistake, where a sweep frees nothing - sweeps at 1000, 2000, 4000 rows rather than on every registration
   * past the first thousand. Without the doubling that case costs a full re-read of the pending set per row, which
   * is quadratic in the number of rows written; with it the total sweep work stays linear.
   */
  private int sweepAt = SWEEP_THRESHOLD;

  /** False between {@link #close()} and the next {@link #open}, so a stale reference cannot defer anything. */
  private boolean active = false;

  /** True while this scope is counted in {@link #ARMED_SCOPES}, so it is counted in and out exactly once. */
  private boolean armed = false;

  /** Nesting depth of {@link #patternCreate}: greater than zero while a pattern clause is writing its elements. */
  private int patternCreateDepth = 0;

  /** Handed to every {@link #patternCreate} caller, so entering a pattern region allocates nothing. */
  private final PatternCreate patternCreateToken = () -> patternCreateDepth--;

  /** The end of a {@link #patternCreate} region. */
  public interface PatternCreate extends AutoCloseable {
    @Override
    void close();
  }

  private DeferredExistenceChecks(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Opens a scope for the statement about to run on this thread, or returns {@code null} when one is already open -
   * in which case the caller is a nested plan and must neither check nor close it.
   * <p>
   * The scope is created once per thread and database and then reused, rather than allocated per statement: it is
   * opened for every openCypher write statement, whether or not the database defines a single existence constraint,
   * so an allocation here would be one more object per write on a path whose whole point is not to cost anything.
   * What a reused scope carries between statements is two empty collections.
   */
  public static DeferredExistenceChecks open(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context == null)
      return null;

    DeferredExistenceChecks scope = context.getDeferredExistenceChecks();
    if (scope == null) {
      scope = new DeferredExistenceChecks(database);
      context.setDeferredExistenceChecks(scope);
    } else if (scope.active)
      // A nested plan: it contributes to the statement's scope and neither checks nor closes it.
      return null;

    scope.active = true;
    return scope;
  }

  /** Whether any scope in this JVM is currently holding a provisional record. See {@link #ARMED_SCOPES}. */
  static boolean anyScopeArmed() {
    return ARMED_SCOPES.get() > 0;
  }

  /**
   * Detaches this scope from the thread. Always called in a {@code finally}: a statement that failed for any other
   * reason must not leave the relaxation in place for whatever runs next on this thread.
   */
  public void close() {
    active = false;
    unresolved.clear();
    pending.clear();
    patternCreateDepth = 0;
    sweepAt = SWEEP_THRESHOLD;
    disarm();
  }

  /**
   * Marks the region in which a {@code CREATE}/{@code MERGE} pattern writes its own nodes and relationships - the
   * only records a later clause of the same statement can still complete, and therefore the only ones this scope
   * ever takes responsibility for.
   * <p>
   * Everything else a statement writes is validated exactly when it always was, which matters beyond keeping the
   * change small: a {@code CALL} procedure that writes a record and reports the refusal itself (as
   * {@code apoc.refactor.cloneNodesWithRelationships} does, in its {@code error} yield field) needs that refusal
   * to reach it at the write, where it can still be caught and attributed to the one input row it belongs to.
   *
   * @return a token to close when the pattern is done, or null when no scope is open on this thread - callers use
   * it as a try-with-resources resource, which skips a null
   */
  public static PatternCreate patternCreate(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context == null)
      return null;

    final DeferredExistenceChecks scope = context.getDeferredExistenceChecks();
    if (scope == null || !scope.active)
      return null;

    scope.patternCreateDepth++;
    return scope.patternCreateToken;
  }

  /**
   * Asks the scope open on this thread, if any, to take responsibility for an existence constraint the document
   * does not satisfy.
   *
   * @return true when the check has been deferred to the end of the statement and the caller must not raise
   */
  static boolean defer(final MutableDocument document) {
    final DatabaseInternal database = databaseOf(document);
    if (database == null)
      return false;

    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context == null)
      return false;

    final DeferredExistenceChecks scope = context.getDeferredExistenceChecks();
    if (scope == null || !scope.active)
      return false;

    // An embedded document has no identity of its own and never will: it is part of the record that holds it, not
    // a record. It is validated from inside its owner's validate(), which for a pattern element runs inside the
    // pattern-create region, so without this it would be deferred like any other identity-less document - and then
    // never resolved, never completed and never checked, which is a constraint silently dropped rather than
    // deferred. Nothing a later clause does could complete it either, since there is no RID to address it by.
    if (document instanceof EmbeddedDocument)
      return false;

    final RID rid = document.getIdentity();
    if (rid == null) {
      // A creation: provisional from here until the statement ends, but only when it is a pattern clause writing
      // its own elements - see patternCreate. Any other creation is validated where it is written.
      if (scope.patternCreateDepth == 0)
        return false;

      scope.register(document);
      return true;
    }

    // An update: deferred only while this very record is still provisional. An update to any other record is a
    // complete write of its own and is validated as it always was.
    return scope.isPending(rid);
  }

  /**
   * Forgets a record that has just validated cleanly, so the pending set holds only what is still incomplete.
   * Called on the ordinary update path, where the {@code SET} that completes a provisional record runs.
   */
  static void completed(final MutableDocument document) {
    final RID rid = document.getIdentity();
    if (rid == null)
      return;

    final DatabaseInternal database = databaseOf(document);
    if (database == null)
      return;

    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context == null)
      return;

    final DeferredExistenceChecks scope = context.getDeferredExistenceChecks();
    if (scope != null && scope.active) {
      // Resolved first: a record completed by the very next write after its creation - the common
      // MERGE ... SET shape - is still waiting for its RID to be picked up, and forgetting it here rather than at
      // the next sweep is what keeps the pending set at the size of the write pipeline's window instead of the
      // statement's row count.
      scope.resolve();
      scope.pending.remove(rid);

      // Nothing incomplete left, so this scope stops making every other write in the JVM look for it. Without this
      // the counter would stay up for the rest of the statement, and a bulk upsert - which holds a provisional
      // record for a moment per row, for as long as the import runs - would tax every other database's writes for
      // its whole duration, which is the opposite of what the counter is for. The atomic is paid only when the
      // state actually changes, and the write pipeline batches, so it is one disarm per batch rather than per row.
      if (scope.isEmpty())
        scope.disarm();
    }
  }

  /**
   * Whether anything at all was deferred. The caller skips {@link #check()} when nothing was, which is the normal
   * case for a statement that writes no provisional record.
   */
  public boolean isEmpty() {
    return pending.isEmpty() && unresolved.isEmpty();
  }

  /**
   * Enforces, now that the statement has finished, every existence constraint that was deferred during it.
   * <p>
   * A record that has been completed in the meantime is forgotten. A record that has not is deleted - it was
   * created by this statement and this statement never completed it - and a {@link ValidationException} naming
   * every offending property is raised.
   */
  public void check() {
    // Unlike discard(), takeIncomplete() is not wrapped here, and deliberately: this runs on the success path,
    // where there is no earlier exception for a failure to displace, and a database that cannot even re-read the
    // records it just wrote should say so rather than have it logged and swallowed. If it does throw, the caller's
    // catch hands it to discard() - the guarded one - and close() clears this scope in its finally either way.
    final StringJoiner violations = new StringJoiner("; ");
    final List<RID> incomplete = takeIncomplete(violations);
    if (incomplete.isEmpty())
      return;

    // Best effort, like discard()'s: the ValidationException below names the constraint the statement actually
    // broke, and a failure to tidy up after it must not take its place.
    int removed = 0;
    try {
      removed = deleteProvisionalRecords(incomplete);
    } catch (final RuntimeException | Error e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not take back the incomplete records of a statement that failed its existence constraints", e);
    }

    // What the message claims is what actually happened. Taking the records back is best effort - a delete can
    // fail, and the whole cleanup transaction can - so an exception that always said they were gone would send an
    // operator looking for records that are still there.
    final String outcome;
    if (removed == incomplete.size())
      outcome = incomplete.size() == 1 ? ". The record has been removed" : ". They have been removed";
    else if (removed == 0)
      outcome = incomplete.size() == 1 ?
          ". The record could not be removed and is still in the database" :
          ". They could not be removed and are still in the database";
    else
      outcome = ". " + removed + " of them have been removed; the rest are still in the database";

    throw new ValidationException(
        (incomplete.size() == 1 ?
            "A record created by this statement was left incomplete: " :
            incomplete.size() + " records created by this statement were left incomplete: ") + violations + outcome);
  }

  /**
   * Takes back the provisional records of a statement that failed for some other reason, quietly: the exception
   * already on its way out is the one the caller has to see, and it is more informative than a second one saying
   * the record the failed statement never finished writing is indeed unfinished. Called on the failure path so the
   * invariant is the same on both - a record that never satisfied its existence constraints does not survive the
   * statement that created it.
   */
  public void discard() {
    // Nothing thrown in here may leave this method. The caller is inside a catch block, on its way to rethrowing
    // the failure the statement actually hit, and everything below touches a database that failure may just have
    // left in a state where a read or a transaction raises something of its own: takeIncomplete() reads each
    // pending record back, and the delete joins the current transaction. Letting one of those escape would replace
    // the real cause with an unrelated one - the exact opposite of what this method is for.
    try {
      deleteProvisionalRecords(takeIncomplete(new StringJoiner("; ")));
    } catch (final RuntimeException | Error e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not take back the incomplete records of a statement that failed for another reason", e);
    } finally {
      pending.clear();
      disarm();
    }
  }

  /**
   * Empties the pending set, returning the records still not satisfying their existence constraints and describing
   * each of them into {@code violations}.
   */
  private List<RID> takeIncomplete(final StringJoiner violations) {
    resolve();
    if (pending.isEmpty())
      return List.of();

    final List<RID> incomplete = new ArrayList<>();
    for (final RID rid : pending) {
      final Document record;
      try {
        record = (Document) database.lookupByRID(rid, true);
      } catch (final RecordNotFoundException | ClassCastException e) {
        // Rolled back, or deleted later in the same statement: there is nothing left to constrain.
        continue;
      }

      final String violation = firstUnmetExistenceConstraint(record);
      if (violation == null)
        continue;

      violations.add(violation);
      incomplete.add(rid);
    }

    pending.clear();
    disarm();
    return incomplete;
  }

  /**
   * The first existence constraint the record does not satisfy, phrased for the end of the statement, or
   * {@code null} when it satisfies all of them. The rule itself is
   * {@link DocumentValidator#unmetExistenceConstraint}, shared with the write path so the question asked here is
   * the same one that was deferred there.
   */
  private static String firstUnmetExistenceConstraint(final Document record) {
    return record.getType().getPolymorphicProperties().stream()
        .map(property -> describeUnmetExistenceConstraint(record, property))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  /** How one unsatisfied existence constraint reads at the end of the statement, or null when it is satisfied. */
  private static String describeUnmetExistenceConstraint(final Document record, final Property property) {
    final DocumentValidator.ExistenceConstraint unmet = DocumentValidator.unmetExistenceConstraint(record, property);
    if (unmet == null)
      return null;

    final String named = "property '" + record.getType().getName() + "." + property.getName() + "'";
    return unmet == DocumentValidator.ExistenceConstraint.MANDATORY ?
        named + " is mandatory, but was never set (record " + record.getIdentity() + ")" :
        named + " cannot be null (record " + record.getIdentity() + ")";
  }

  /**
   * Takes back the creations this scope allowed through. Best effort per record and never masks the validation
   * error that is on its way out: a record that cannot be deleted is logged, because the exception the caller is
   * about to see is the more useful of the two.
   */
  private int deleteProvisionalRecords(final List<RID> rids) {
    if (rids.isEmpty())
      return 0;

    // One transaction for the whole set rather than one per record: a statement that wrote thousands of records it
    // never completed would otherwise pay thousands of commits on its way out.
    //
    // joinCurrentTx is true because it has to be: inside an explicit transaction the provisional records are not
    // committed yet, so a private transaction could not even see them. That makes it critical that nothing thrown
    // by a delete escapes this block - LocalDatabase.transaction() rolls the current transaction back on any
    // exception it does not recognise as retryable, and when it has joined the caller's transaction, "the current
    // transaction" is a session-long explicit one holding work this statement knows nothing about. Discarding that
    // to report a record we could not tidy up would be silent data loss well beyond the failure being reported, so
    // each delete swallows and logs its own failure and the sweep continues.
    final int[] removed = { 0 };
    database.transaction(() -> {
      removed[0] = 0;
      for (final RID rid : rids)
        if (deleteProvisionalRecord(rid))
          removed[0]++;
    }, true);

    return removed[0];
  }

  /**
   * Deletes one provisional record, best effort: see {@link #deleteProvisionalRecords} for why it cannot throw.
   *
   * @return whether the record is gone, which a record that was already gone also satisfies
   */
  private boolean deleteProvisionalRecord(final RID rid) {
    try {
      final Record record = database.lookupByRID(rid, false);
      if (record != null)
        database.deleteRecord(record);
      return true;
    } catch (final RecordNotFoundException e) {
      // Already gone - rolled back with the transaction that created it, or deleted by the statement itself.
      return true;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not remove the incomplete record %s left by a statement that failed its existence constraints", e, rid);
      return false;
    }
  }

  private void register(final MutableDocument document) {
    // One document, one registration: validate() walks the properties, so a record missing two mandatory ones
    // defers twice in a row for the same instance.
    if (!unresolved.isEmpty() && unresolved.getLast() == document)
      return;

    resolve();
    if (pending.size() >= sweepAt) {
      sweep();
      sweepAt = Math.max(SWEEP_THRESHOLD, pending.size() * 2);
    }
    unresolved.add(document);

    if (!armed) {
      armed = true;
      ARMED_SCOPES.incrementAndGet();
    }
  }

  private void disarm() {
    if (armed) {
      armed = false;
      ARMED_SCOPES.decrementAndGet();
    }
  }

  private boolean isPending(final RID rid) {
    resolve();
    return pending.contains(rid);
  }

  /**
   * Moves into {@link #pending} the records whose creation has since assigned them a RID. One that still has none
   * is kept: its creation is on the stack right now (a create fired from inside another record's validation, for
   * instance), and dropping it here would lose the only reference to it.
   */
  private void resolve() {
    if (unresolved.isEmpty())
      return;

    for (final Iterator<MutableDocument> it = unresolved.iterator(); it.hasNext(); ) {
      final RID rid = it.next().getIdentity();
      if (rid != null) {
        pending.add(rid);
        it.remove();
      }
    }
  }

  /** Drops from {@link #pending} every record the statement has completed since it was registered. */
  private void sweep() {
    for (final Iterator<RID> it = pending.iterator(); it.hasNext(); ) {
      final RID rid = it.next();
      try {
        if (firstUnmetExistenceConstraint((Document) database.lookupByRID(rid, true)) == null)
          it.remove();
      } catch (final RecordNotFoundException | ClassCastException e) {
        it.remove();
      }
    }
  }

  private static DatabaseInternal databaseOf(final MutableDocument document) {
    final Database database = document.getDatabase();
    return database instanceof DatabaseInternal internal ? internal : null;
  }

  private static DatabaseContext.DatabaseContextTL contextOf(final DatabaseInternal database) {
    return DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
  }
}
