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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DocumentValidator.ReadonlyDeferral;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12722: {@link DocumentValidator#deferReadonlyChecks} lets a restore complete a READONLY property in a second write. The scope
 * is for one record instance and the properties it names, on the thread that opened it, and for nothing else.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class DocumentValidatorReadonlyDeferralTest extends TestHelper {
  private RID first;
  private RID second;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Holder");
    database.command("sql", "CREATE PROPERTY Holder.a STRING (readonly true)");
    database.command("sql", "CREATE PROPERTY Holder.b STRING (readonly true)");
    database.command("sql", "CREATE PROPERTY Holder.must STRING");
    database.transaction(() -> {
      first = database.newDocument("Holder").set("a", "a0").set("b", "b0").set("must", "m").save().getIdentity();
      second = database.newDocument("Holder").set("a", "a0").set("b", "b0").set("must", "m").save().getIdentity();
    });
    database.command("sql", "ALTER PROPERTY Holder.must MANDATORY true");
  }

  @Test
  void withoutAScopeTheRewriteIsRefused() {
    assertRefused(first, "a");
  }

  @Test
  void theNamedPropertyOfTheNamedRecordIsRewritten() {
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        doc.save();
      }
    });
    assertThat(valueOf(first, "a")).isEqualTo("a1");
  }

  @Test
  void anotherReadonlyPropertyOfTheSameRecordIsStillRefused() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1").set("b", "b1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        doc.save();
      }
    })).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.b").hasMessageContaining("immutable");
    assertThat(valueOf(first, "a")).isEqualTo("a0");
    assertThat(valueOf(first, "b")).isEqualTo("b0");
  }

  /** What a listener or a trigger of the deferred write does to another record is as refused as it is outside the scope. */
  @Test
  void anotherRecordWrittenInTheScopeIsStillRefused() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        modify(second).set("a", "a1").save();
      }
    })).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
    assertThat(valueOf(second, "a")).isEqualTo("a0");
  }

  /** The scope is for the instance it was opened with: the same record loaded again is another write. */
  @Test
  void anotherCopyOfTheSameRecordIsStillRefused() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        modify(first).set("a", "a2").save();
      }
    })).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
    assertThat(valueOf(first, "a")).isEqualTo("a0");
  }

  @Test
  void everyOtherConstraintIsStillChecked() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      doc.remove("must");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        doc.save();
      }
    })).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.must");
  }

  @Test
  void theCheckIsBackWhenTheScopeEndsWhateverHappenedInIt() {
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        throw new IllegalStateException("the restore failed");
      } catch (final IllegalStateException expected) {
        // the scope was closed on the way out
      }
      assertThatThrownBy(doc::save).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
    });
  }

  @Test
  void scopesNest() {
    database.transaction(() -> {
      final MutableDocument one = modify(first).set("a", "a1");
      final MutableDocument two = modify(second).set("b", "b1");
      try (final ReadonlyDeferral outer = DocumentValidator.deferReadonlyChecks(one, Set.of("a"))) {
        try (final ReadonlyDeferral inner = DocumentValidator.deferReadonlyChecks(two, Set.of("b"))) {
          // the inner scope does not hide the outer one
          one.save();
          two.save();
        }
        // the inner scope's end does not end the outer one, and ends its own
        one.set("a", "a2").save();
        assertThatThrownBy(() -> two.set("b", "b2").save()).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
        two.set("b", "b1");
      }
      assertThatThrownBy(() -> one.set("a", "a3").save()).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
      one.set("a", "a2");
    });
    assertThat(valueOf(first, "a")).isEqualTo("a2");
    assertThat(valueOf(second, "b")).isEqualTo("b1");
  }

  /** A second close ends nothing: it cannot lift the deferral of an enclosing scope. */
  @Test
  void closingAScopeTwiceEndsItOnce() {
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral outer = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        final ReadonlyDeferral inner = DocumentValidator.deferReadonlyChecks(doc, Set.of("b"));
        inner.close();
        inner.close();
        // the outer scope is still open
        doc.save();
      }
    });
    assertThat(valueOf(first, "a")).isEqualTo("a1");
    assertRefused(first, "a");
  }

  /** A scope closed before one opened inside it leaves the inner one open, and the inner one's close leaves nothing behind. */
  @Test
  void aScopeClosedBeforeTheOneInsideItEndsOnlyItself() {
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1").set("b", "b1");
      final ReadonlyDeferral outer = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"));
      final ReadonlyDeferral inner = DocumentValidator.deferReadonlyChecks(doc, Set.of("b"));
      outer.close();
      assertThatThrownBy(doc::save).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.a");
      doc.set("a", "a0");
      doc.save();
      inner.close();
      assertThatThrownBy(() -> doc.set("b", "b2").save()).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.b");
      doc.set("b", "b1");
    });
    assertThat(valueOf(first, "a")).isEqualTo("a0");
    assertThat(valueOf(first, "b")).isEqualTo("b1");
  }

  /** A scope closed from another thread is a mistake that is reported, and the thread that opened it keeps its deferral. */
  @Test
  void aScopeClosedFromAnotherThreadIsRefused() {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral scope = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        final Thread thread = new Thread(() -> {
          try {
            scope.close();
          } catch (final Throwable t) {
            failure.set(t);
          }
        });
        thread.start();
        join(thread);
        // still deferred here
        doc.save();
      }
    });
    assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("thread that opened it");
    assertThat(valueOf(first, "a")).isEqualTo("a1");
    assertRefused(first, "a");
  }

  /** A scope open on one thread relaxes nothing for another one, whatever it writes. */
  @Test
  void aScopeBelongsToItsThread() {
    final AtomicReference<Throwable> otherThreadFailure = new AtomicReference<>();
    database.transaction(() -> {
      final MutableDocument doc = modify(first).set("a", "a1");
      try (final ReadonlyDeferral ignored = DocumentValidator.deferReadonlyChecks(doc, Set.of("a"))) {
        final Thread thread = new Thread(() -> {
          try {
            DocumentValidator.validate(doc);
          } catch (final Throwable t) {
            otherThreadFailure.set(t);
          }
        });
        thread.start();
        join(thread);
      }
    });
    assertThat(otherThreadFailure.get()).isInstanceOf(ValidationException.class).hasMessageContaining("immutable");
  }

  private MutableDocument modify(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().modify();
  }

  private String valueOf(final RID rid, final String property) {
    final String[] value = new String[1];
    database.transaction(() -> value[0] = database.lookupByRID(rid, true).asDocument().getString(property));
    return value[0];
  }

  private void assertRefused(final RID rid, final String property) {
    assertThatThrownBy(() -> database.transaction(() -> modify(rid).set(property, "refused").save())).isInstanceOf(
        ValidationException.class).hasMessageContaining("immutable");
  }

  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
