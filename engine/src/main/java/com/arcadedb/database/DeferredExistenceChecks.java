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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
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
 * Scopes nest: only the outermost {@link #open} returns a scope, so a nested plan (a {@code CALL} subquery, a
 * {@code FOREACH} body, a UNION branch) contributes its provisional records to the enclosing statement's scope and
 * never checks or closes it.
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
   */
  private static final AtomicInteger ARMED_SCOPES = new AtomicInteger();

  /** The end of a {@link #patternCreate} region. */
  public interface PatternCreate extends AutoCloseable {
    @Override
    void close();
  }

  private final DatabaseInternal database;

  /**
   * Records registered before their RID was assigned: {@link LocalDatabase#createRecordNoLock} validates the record
   * before it has an identity, so the instance is what there is to hold on to at that point. Resolved into
   * {@link #pending} at the next registration, by which time the creation that triggered it has returned.
   */
  private final List<MutableDocument> unresolved = new ArrayList<>();

  /** Provisional records by RID, in creation order so the failure reports them in the order they were written. */
  private final Set<RID> pending = new LinkedHashSet<>();

  /** True while this scope is counted in {@link #ARMED_SCOPES}, so it is counted in and out exactly once. */
  private boolean armed = false;

  /** Nesting depth of {@link #patternCreate}: greater than zero while a pattern clause is writing its elements. */
  private int patternCreateDepth = 0;

  /** Handed to every {@link #patternCreate} caller, so entering a pattern region allocates nothing. */
  private final PatternCreate patternCreateToken = () -> patternCreateDepth--;

  private DeferredExistenceChecks(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Opens a scope for the statement about to run on this thread, or returns {@code null} when one is already open -
   * in which case the caller is a nested plan and must neither check nor close it.
   */
  public static DeferredExistenceChecks open(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context == null || context.getDeferredExistenceChecks() != null)
      return null;

    final DeferredExistenceChecks scope = new DeferredExistenceChecks(database);
    context.setDeferredExistenceChecks(scope);
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
    final DatabaseContext.DatabaseContextTL context = contextOf(database);
    if (context != null && context.getDeferredExistenceChecks() == this)
      context.setDeferredExistenceChecks(null);

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
    if (scope == null)
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
    if (scope == null)
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
    if (scope != null) {
      // Resolved first: a record completed by the very next write after its creation - the common
      // MERGE ... SET shape - is still waiting for its RID to be picked up, and forgetting it here rather than at
      // the next sweep is what keeps the pending set at the size of the write pipeline's window instead of the
      // statement's row count.
      scope.resolve();
      scope.pending.remove(rid);
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
    final StringJoiner violations = new StringJoiner("; ");
    final List<RID> incomplete = takeIncomplete(violations);
    if (incomplete.isEmpty())
      return;

    deleteProvisionalRecords(incomplete);

    throw new ValidationException(
        "A record created by this statement was left incomplete: " + violations + ". The record has been removed");
  }

  /**
   * Takes back the provisional records of a statement that failed for some other reason, quietly: the exception
   * already on its way out is the one the caller has to see, and it is more informative than a second one saying
   * the record the failed statement never finished writing is indeed unfinished. Called on the failure path so the
   * invariant is the same on both - a record that never satisfied its existence constraints does not survive the
   * statement that created it.
   */
  public void discard() {
    deleteProvisionalRecords(takeIncomplete(new StringJoiner("; ")));
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
   * The first existence constraint the record does not satisfy, or {@code null} when it satisfies all of them.
   * Mirrors the {@code MANDATORY}/{@code NOTNULL} arms of
   * {@link DocumentValidator#validateField(MutableDocument, Property, long)} - the rules live there, this asks them
   * of a record that is no longer a {@link MutableDocument}.
   */
  private static String firstUnmetExistenceConstraint(final Document record) {
    final DocumentType type = record.getType();
    for (final Property property : type.getPolymorphicProperties()) {
      final String name = property.getName();
      if (property.isMandatory() && !record.has(name))
        return "property '" + type.getName() + "." + name + "' is mandatory, but was never set (record " + record
            .getIdentity() + ")";
      if (property.isNotNull() && record.has(name) && record.get(name) == null)
        return "property '" + type.getName() + "." + name + "' cannot be null (record " + record.getIdentity() + ")";
    }
    return null;
  }

  /**
   * Takes back the creations this scope allowed through. Best effort per record and never masks the validation
   * error that is on its way out: a record that cannot be deleted is logged, because the exception the caller is
   * about to see is the more useful of the two.
   */
  private void deleteProvisionalRecords(final List<RID> rids) {
    for (final RID rid : rids) {
      try {
        database.transaction(() -> {
          final Record record = database.lookupByRID(rid, false);
          if (record != null)
            database.deleteRecord(record);
        }, true);
      } catch (final RecordNotFoundException e) {
        // Already gone - rolled back with the transaction that created it, or deleted by the statement itself.
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not remove the incomplete record %s left by a statement that failed its existence constraints", e, rid);
      }
    }
  }

  private void register(final MutableDocument document) {
    resolve();
    if (pending.size() >= SWEEP_THRESHOLD)
      sweep();
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
