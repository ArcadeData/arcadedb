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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code BEFORE READ} trigger fires from inside the read itself, and that is the whole difficulty: it is the one
 * timing that cannot be handed the record, because the record is what the read has not produced yet.
 * <p>
 * The adapter used to load it anyway - {@code rid.asDocument()} - from {@code onBeforeRead}, which
 * {@code LocalBucket.getRecordInternal} fires at its very top. So the load re-entered the read, which fired the
 * trigger, which loaded... Creating the trigger succeeded and then every read of that type died with a
 * {@code StackOverflowError} wrapped in {@code DatabaseOperationException: Error during read lock}. The DDL is a
 * documented, parseable statement, so the feature was reachable, broken, and untested: the only coverage
 * ({@code TriggerSQLTest.allEventTypes}) creates all eight timing/event pairs and asserts the schema holds eight
 * triggers, never reading a record with one installed.
 * <p>
 * The contract now matches what the timing can actually offer: a {@code BEFORE READ} trigger is given the RID and
 * nothing else. It can decide from the identity - and veto the read by returning false, which surfaces as
 * {@code RecordNotFoundException} - but it cannot inspect content, because content is exactly what does not exist
 * yet. Every other timing still receives the record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BeforeReadTriggerTest extends TestHelper {
  private static final String TYPE_NAME  = "Reader";
  private static final String OTHER_TYPE = "Untriggered";

  /** Populated by {@link RecordingBeforeReadTrigger}; static because the trigger is instantiated by class name. */
  static final List<RID>     SEEN   = new ArrayList<>();
  /** Flipped by a test to make {@link RecordingBeforeReadTrigger} veto the read. */
  static final AtomicBoolean ALLOW  = new AtomicBoolean(true);

  /** A BEFORE READ trigger sees the RID and nothing else - which is all this timing can honestly provide. */
  public static class RecordingBeforeReadTrigger implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final com.arcadedb.database.Record record,
        final com.arcadedb.database.Record oldRecord) {
      throw new IllegalStateException("a BEFORE READ trigger must not be routed through the record-based hook");
    }

    @Override
    public boolean executeBeforeRead(final Database database, final RID rid) {
      SEEN.add(rid);
      return ALLOW.get();
    }
  }

  /** Counts listener invocations, so a re-entrant fire is visible as a number rather than as a stack overflow. */
  static final AtomicInteger FIRED        = new AtomicInteger();
  /** A second record of the same type, for the AFTER-side body to read. */
  static final AtomicReference<RID> OTHER_RECORD = new AtomicReference<>();

  /** Re-reads the very record whose read fired it. */
  public static class SelfReadingBeforeReadTrigger implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final com.arcadedb.database.Record record,
        final com.arcadedb.database.Record oldRecord) {
      return true;
    }

    @Override
    public boolean executeBeforeRead(final Database database, final RID rid) {
      // Reads only on the FIRST invocation. Without the guard the nested read fires this listener again, the count
      // reaches 2 and the recursion stops HERE - deliberately, so the regression surfaces as a failed assertion on
      // the count. Left unbounded it recurses until the stack ends, and a StackOverflowError that deep takes the
      // surefire fork with it: the build then reports "Tests run: 0" and passes, which is the one failure mode a
      // regression test must not have.
      if (FIRED.incrementAndGet() > 1)
        return true;
      database.lookupByRID(rid, true).asDocument().get("name");
      return true;
    }
  }

  /** Reads a DIFFERENT record of the same type, which is what can re-fire an AFTER READ listener. */
  public static class SelfReadingAfterReadTrigger implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final com.arcadedb.database.Record record,
        final com.arcadedb.database.Record oldRecord) {
      // Bounded for the same reason as the BEFORE twin.
      if (FIRED.incrementAndGet() > 1)
        return true;
      database.lookupByRID(OTHER_RECORD.get(), true).asDocument().get("name");
      return true;
    }
  }

  @BeforeEach
  void resetTriggerState() {
    SEEN.clear();
    ALLOW.set(true);
    FIRED.set(0);
  }

  /**
   * THE REGRESSION. Before the fix this did not fail an assertion - it died with a {@code StackOverflowError}, so
   * the type became unreadable the moment the trigger was created.
   */
  @Test
  void aBeforeReadTriggerDoesNotMakeTheTypeUnreadable() {
    final RID rid = createRecordAndTrigger();

    database.transaction(() -> assertThat(database.lookupByRID(rid, true).asDocument().<String>get("name"))
        .as("a BEFORE READ trigger must not stop the read it fires from").isEqualTo("first"));
  }

  /** ...and it actually fires, with the RID of the record being read. */
  @Test
  void theTriggerIsGivenTheRidBeingRead() {
    final RID rid = createRecordAndTrigger();

    database.transaction(() -> database.lookupByRID(rid, true).asDocument().get("name"));

    assertThat(SEEN).as("the trigger must fire, and receive the identity of the record being read").contains(rid);
  }

  /** Returning false aborts the read, which the bucket surfaces as a missing record. */
  @Test
  void theTriggerCanVetoTheRead() {
    final RID rid = createRecordAndTrigger();
    ALLOW.set(false);

    database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(rid, true).asDocument().get("name"))
        .as("a vetoed read must not return the record").isInstanceOf(RecordNotFoundException.class));
  }

  /** The listener is registered on its own type's registry, so another type's records never reach it. */
  @Test
  void theTriggerDoesNotFireForAnotherType() {
    createRecordAndTrigger();

    final RID[] other = new RID[1];
    database.transaction(() -> other[0] = database.newDocument(OTHER_TYPE).set("name", "elsewhere").save().getIdentity());

    database.transaction(() -> database.lookupByRID(other[0], true).asDocument().get("name"));

    assertThat(SEEN).as("a trigger on %s must not fire for %s", TYPE_NAME, OTHER_TYPE).isEmpty();
  }

  /**
   * The placeholder path. A record that outgrows its page is relocated, and {@code LocalBucket} then re-enters
   * {@code getRecordInternal} with the placeholder POINTER - a different, bare RID - firing the trigger a second
   * time for the same logical read. Worth pinning because it is the one shape where the RID the trigger sees is
   * not the one the caller asked for.
   */
  @Test
  void aRelocatedRecordIsStillReadable() {
    final RID rid = createRecordAndTrigger();

    // Grow the record far past its original footprint so the bucket has to relocate it behind a placeholder.
    database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("payload", "x".repeat(60_000))
        .save());

    database.transaction(() -> {
      final var doc = database.lookupByRID(rid, true).asDocument();
      assertThat(doc.<String>get("name")).isEqualTo("first");
      assertThat(doc.getString("payload")).hasSize(60_000);
    });

    assertThat(SEEN).as("the trigger still fires for a relocated record").contains(rid);
  }

  /**
   * A SQL BEFORE READ trigger runs its body without the record, and must not recurse either.
   * <p>
   * The body is {@code SELECT 1} rather than something reading the type, and it is not laziness in the test: it is
   * a projection against no type, so consuming it (see {@link SQLTriggerExecutor}) touches no record and cannot
   * re-enter the read. The recursion this class guards against is therefore exercised with a Java trigger below,
   * which really does read.
   */
  @Test
  void aSqlBeforeReadTriggerRunsWithoutTheRecord() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity());

    database.command("sql", "CREATE TRIGGER sqlRead BEFORE READ ON TYPE " + TYPE_NAME + " EXECUTE SQL 'SELECT 1'");

    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().<String>get("name"))
        .isEqualTo("first"));
  }

  /**
   * The SQL arm of the veto contract, matching {@link #theTriggerCanVetoTheRead()} (Java) and
   * {@link #aJavaScriptBeforeReadTriggerSeesTheRidAndCanVeto()} (JavaScript): a body that evaluates to a single
   * scalar {@code false} aborts the read, which the bucket surfaces as {@code RecordNotFoundException}.
   */
  @Test
  void aSqlBeforeReadTriggerCanVetoTheRead() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity());

    database.command("sql", "CREATE TRIGGER sqlVetoRead BEFORE READ ON TYPE " + TYPE_NAME + " EXECUTE SQL 'SELECT false'");

    database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(rid[0], true).asDocument().get("name"))
        .as("a SQL body evaluating to a single false must veto the read").isInstanceOf(RecordNotFoundException.class));
  }

  /**
   * The JavaScript arm. It gets {@code rid}/{@code $rid} and no {@code record}, and - like the other two - must not
   * re-enter the read. Returning false from the script vetoes it, which is the JS executor's existing contract for
   * every other timing.
   */
  @Test
  void aJavaScriptBeforeReadTriggerSeesTheRidAndCanVeto() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity());

    // Polarity matters here. The script ALLOWS only when it can see the RID and it is the expected one, so the read
    // succeeding is the evidence that the binding works. Written the other way round - veto when the RID matches -
    // the test would pass with `rid` bound to null too, because a script that cannot see the RID also vetoes;
    // verified by making the executor bind null and watching the veto-shaped version stay green.
    database.command("sql", "CREATE TRIGGER jsRead BEFORE READ ON TYPE " + TYPE_NAME
        + " EXECUTE JAVASCRIPT 'rid != null && rid.toString() === \"" + rid[0] + "\"'");

    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().<String>get("name"))
        .as("the script must see the RID of the record being read, and allow it").isEqualTo("first"));
  }

  /**
   * A trigger body that re-reads the record being read. The adapter no longer loads it, but the body is arbitrary
   * user code and a lookup is a reasonable thing to put in one - which rebuilds the loop by hand: the lookup reads,
   * the read fires the trigger, the trigger looks up...
   * <p>
   * A read-event listener's own reads no longer fire read events, so the body runs ONCE, which is what the count
   * asserts. The body deliberately stops recursing after the second entry: measured, an unbounded version does
   * reproduce the bug, but it does so by killing the surefire fork, and the build then reports "Tests run: 0" and
   * PASSES. Bounding it turns the same regression into a failed assertion that says 2 instead of 1.
   */
  @Test
  void aBeforeReadTriggerBodyThatReadsDoesNotRecurse() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity());

    database.command("sql", "CREATE TRIGGER selfReading BEFORE READ ON TYPE " + TYPE_NAME
        + " EXECUTE JAVA '" + SelfReadingBeforeReadTrigger.class.getName() + "'");

    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().<String>get("name"))
        .as("a trigger body that reads must not recurse").isEqualTo("first"));

    assertThat(FIRED).as("the listener's own read must not fire the listener again").hasValue(1);
  }

  /**
   * The AFTER side, where the recursion is milder but the rule is the same. The body reads a DIFFERENT record of the
   * same type - reading the one it was handed would be served from the transaction cache and prove nothing - so
   * without the guard the trigger fires twice for one logical read.
   */
  @Test
  void anAfterReadTriggerBodyThatReadsDoesNotRefireTheTrigger() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    final RID[] rid = new RID[2];
    database.transaction(() -> {
      rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity();
      rid[1] = database.newDocument(TYPE_NAME).set("name", "second").save().getIdentity();
    });
    OTHER_RECORD.set(rid[1]);

    database.command("sql", "CREATE TRIGGER selfReadingAfter AFTER READ ON TYPE " + TYPE_NAME
        + " EXECUTE JAVA '" + SelfReadingAfterReadTrigger.class.getName() + "'");

    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().<String>get("name"))
        .isEqualTo("first"));

    assertThat(FIRED).as("the listener's own read must not fire the listener again").hasValue(1);
  }

  private RID createRecordAndTrigger() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE DOCUMENT TYPE " + OTHER_TYPE);

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE_NAME).set("name", "first").save().getIdentity());

    database.command("sql", "CREATE TRIGGER javaRead BEFORE READ ON TYPE " + TYPE_NAME
        + " EXECUTE JAVA '" + RecordingBeforeReadTrigger.class.getName() + "'");
    return rid[0];
  }
}
