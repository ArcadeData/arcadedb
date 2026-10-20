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
import com.arcadedb.database.DocumentValidator.LinkTargetTypeDeferral;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10483: a restore that writes records in the order of a file meets a {@code LINK OF T}, {@code LIST OF T} or {@code MAP OF T}
 * before the record it names, and must be able to suspend the check of the link's target type for that write, and only that check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10483DeferredLinkTargetTypeCheckTest extends TestHelper {
  private RID wanted;
  private RID other;

  @BeforeEach
  void schema() {
    database.command("sql", "CREATE DOCUMENT TYPE Wanted");
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.command("sql", "CREATE DOCUMENT TYPE Holder");
    database.command("sql", "CREATE PROPERTY Holder.link LINK OF Wanted");
    database.command("sql", "CREATE PROPERTY Holder.list LIST OF Wanted");
    database.command("sql", "CREATE PROPERTY Holder.map MAP OF Wanted");
    database.command("sql", "CREATE PROPERTY Holder.name STRING");
    database.transaction(() -> {
      wanted = database.newDocument("Wanted").save().getIdentity();
      other = database.newDocument("Other").save().getIdentity();
    });
  }

  @Test
  void aLinkToTheWrongTypeIsRefusedByDefault() {
    assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class).hasMessageContaining("LINK of 'Wanted'");
    assertThatThrownBy(() -> save(holderListing(other))).isInstanceOf(ValidationException.class).hasMessageContaining("LIST of 'Wanted'");
    assertThatThrownBy(() -> save(holderMapping(other))).isInstanceOf(ValidationException.class).hasMessageContaining("MAP of");
  }

  @Test
  void aSuspendedCheckLetsTheWrongTypeThrough() {
    try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
      save(holderLinking(other));
      save(holderListing(other));
      save(holderMapping(other));
    }
    assertThat(database.countType("Holder", false)).isEqualTo(3L);
  }

  /** The suspension is of the target's type only: the other constraints are checked as always. */
  @Test
  void aSuspendedCheckStillChecksEverythingElse() {
    database.command("sql", "CREATE PROPERTY Holder.must STRING");
    database.command("sql", "ALTER PROPERTY Holder.must MANDATORY true");
    try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
      assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class).hasMessageContaining("Holder.must");
    }
  }

  @Test
  void theCheckIsBackWhenTheScopeEndsWhateverHappenedInIt() {
    try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
      throw new IllegalStateException("the restore failed");
    } catch (final IllegalStateException expected) {
      // the scope was closed on the way out
    }
    assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
  }

  @Test
  void scopesNest() {
    try (final LinkTargetTypeDeferral outer = DocumentValidator.deferLinkTargetTypeChecks()) {
      try (final LinkTargetTypeDeferral inner = DocumentValidator.deferLinkTargetTypeChecks()) {
        save(holderLinking(other));
      }
      // the inner scope's end does not end the outer one
      save(holderLinking(other));
    }
    assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
    assertThat(database.countType("Holder", false)).isEqualTo(2L);
  }

  /** A second close ends nothing: it cannot lift the deferral of an enclosing scope. */
  @Test
  void closingAScopeTwiceEndsItOnce() {
    try (final LinkTargetTypeDeferral outer = DocumentValidator.deferLinkTargetTypeChecks()) {
      final LinkTargetTypeDeferral inner = DocumentValidator.deferLinkTargetTypeChecks();
      inner.close();
      inner.close();
      // the outer scope is still open
      save(holderLinking(other));
    }
    assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
    assertThat(database.countType("Holder", false)).isEqualTo(1L);
  }

  /** A scope closed from another thread is a mistake that is reported, and the thread that opened it keeps its deferral. */
  @Test
  void aScopeClosedFromAnotherThreadIsRefused() throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    try (final LinkTargetTypeDeferral scope = DocumentValidator.deferLinkTargetTypeChecks()) {
      final Thread thread = new Thread(() -> {
        try {
          scope.close();
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      thread.start();
      thread.join();
      // still deferred here
      save(holderLinking(other));
    }
    assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("thread that opened it");
    assertThatThrownBy(() -> save(holderLinking(other))).isInstanceOf(ValidationException.class);
  }

  @Test
  void aScopeBelongsToItsThread() throws Exception {
    final AtomicReference<Throwable> otherThreadFailure = new AtomicReference<>();
    try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
      final Thread thread = new Thread(() -> {
        try {
          save(holderLinking(other));
        } catch (final Throwable t) {
          otherThreadFailure.set(t);
        }
      });
      thread.start();
      thread.join();
    }
    assertThat(otherThreadFailure.get()).isInstanceOf(ValidationException.class);
  }

  /** A link into a bucket no type of this database owns is a refusal that names the property, not a NullPointerException. */
  @Test
  void aLinkIntoNoBucketIsAValidationFailure() {
    final RID nowhere = new RID(9_999, 0L);
    assertThatThrownBy(() -> save(holderLinking(nowhere))).isInstanceOf(ValidationException.class)
        .hasMessageContaining("Holder.link");
    assertThatThrownBy(() -> save(holderListing(nowhere))).isInstanceOf(ValidationException.class)
        .hasMessageContaining("Holder.list");
  }

  @Test
  void aLinkToTheRightTypeIsAcceptedWithOrWithoutTheScope() {
    save(holderLinking(wanted));
    try (final LinkTargetTypeDeferral ignored = DocumentValidator.deferLinkTargetTypeChecks()) {
      save(holderLinking(wanted));
    }
    assertThat(database.countType("Holder", false)).isEqualTo(2L);
  }

  private void save(final MutableDocument holder) {
    database.transaction(holder::save);
  }

  private MutableDocument holderLinking(final RID target) {
    return database.newDocument("Holder").set("link", target);
  }

  private MutableDocument holderListing(final RID target) {
    final List<Object> list = new ArrayList<>();
    list.add(target);
    return database.newDocument("Holder").set("list", list);
  }

  private MutableDocument holderMapping(final RID target) {
    final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("k", target);
    return database.newDocument("Holder").set("map", map);
  }
}
