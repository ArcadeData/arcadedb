/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9904: the bookkeeping of configuration changes whose outcome is unknown - which settings a later change must
 * hold back, and how the fate of a change is settled.
 */
class Issue9904UnresolvedConfigurationChangesTest {

  private static final String A_5 = "{\"configuration\":{\"typeDefaultBuckets\":5},\"removed\":[]}";

  /** A database whose file, barrier and restore the test controls. */
  private static class FakeEnvironment implements UnresolvedConfigurationChanges.Environment {
    final AtomicBoolean saved       = new AtomicBoolean();
    final AtomicBoolean clusterUp   = new AtomicBoolean(true);
    final AtomicInteger restored    = new AtomicInteger();
    final AtomicInteger barriers    = new AtomicInteger();
    final List<String>  restoredDoc = new ArrayList<>();

    @Override
    public boolean isSaved(final String changes) {
      return saved.get();
    }

    @Override
    public void awaitLaterEntryApplied() {
      if (!clusterUp.get())
        throw new ReplicationDispatchedTimeoutException("cluster not reachable");
      barriers.incrementAndGet();
    }

    @Override
    public void restore(final String changes) throws IOException {
      restored.incrementAndGet();
      restoredDoc.add(changes);
    }

    @Override
    public boolean isActive() {
      return true;
    }
  }

  private UnresolvedConfigurationChanges held(final FakeEnvironment environment) {
    final UnresolvedConfigurationChanges changes = new UnresolvedConfigurationChanges("db", new Object(), environment);
    // Registered without starting the resolver thread, which the tests drive by hand
    changes.hold(A_5);
    return changes;
  }

  @Test
  void aHeldSettingDoesNotRideAnotherChange() {
    final UnresolvedConfigurationChanges changes = held(new FakeEnvironment());

    final String shipped = changes.withoutUnresolved(
        "{\"configuration\":{\"typeDefaultBuckets\":5,\"dateImplementation\":\"java.util.Date\"},\"removed\":[]}");

    assertThat(shipped).contains("dateImplementation").doesNotContain("typeDefaultBuckets");
  }

  @Test
  void whenOnlyAHeldSettingIsPendingNothingIsShipped() {
    final UnresolvedConfigurationChanges changes = held(new FakeEnvironment());

    assertThat(changes.withoutUnresolved(A_5)).isNull();
  }

  @Test
  void aHeldSettingChangedAgainTravelsWithItsNewValue() {
    final UnresolvedConfigurationChanges changes = held(new FakeEnvironment());

    final String shipped = changes.withoutUnresolved("{\"configuration\":{\"typeDefaultBuckets\":8},\"removed\":[]}");

    assertThat(shipped).contains("\"typeDefaultBuckets\":8");
  }

  @Test
  void aHeldRemovalIsHeldBackToo() {
    final UnresolvedConfigurationChanges changes = new UnresolvedConfigurationChanges("db", new Object(), new FakeEnvironment());
    changes.hold("{\"configuration\":{},\"removed\":[\"typeDefaultBuckets\"]}");

    assertThat(changes.withoutUnresolved("{\"configuration\":{},\"removed\":[\"typeDefaultBuckets\",\"other\"]}"))
        .contains("other").doesNotContain("typeDefaultBuckets");
  }

  @Test
  void nothingHeldLeavesTheChangesAsTheyAre() {
    final UnresolvedConfigurationChanges changes = new UnresolvedConfigurationChanges("db", new Object(), new FakeEnvironment());

    assertThat(changes.withoutUnresolved(A_5)).isEqualTo(A_5);
    assertThat(changes.withoutUnresolved(null)).isNull();
  }

  @Test
  void aCommittedChangeIsDroppedAndNeverRestored() throws Exception {
    final FakeEnvironment environment = new FakeEnvironment();
    final UnresolvedConfigurationChanges changes = held(environment);
    environment.saved.set(true);

    changes.tryResolve(A_5);

    assertThat(changes.size()).isZero();
    assertThat(environment.restored.get()).isZero();
  }

  @Test
  void aChangeNotInTheFileOnceALaterEntryIsAppliedIsRestored() throws Exception {
    final FakeEnvironment environment = new FakeEnvironment();
    final UnresolvedConfigurationChanges changes = held(environment);

    changes.tryResolve(A_5);

    assertThat(environment.barriers.get()).isEqualTo(1);
    assertThat(environment.restoredDoc).containsExactly(A_5);
    assertThat(changes.size()).isZero();
  }

  @Test
  void aChangeStaysHeldWhileTheClusterCannotSayWhatBecameOfIt() {
    final FakeEnvironment environment = new FakeEnvironment();
    final UnresolvedConfigurationChanges changes = held(environment);
    environment.clusterUp.set(false);

    assertThatThrownBy(() -> changes.tryResolve(A_5)).isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(environment.restored.get()).isZero();
    assertThat(changes.size()).isEqualTo(1);
  }

  @Test
  void anEntryAppliedBetweenTheBarrierAndTheCheckIsNotRestored() throws Exception {
    final FakeEnvironment environment = new FakeEnvironment() {
      @Override
      public void awaitLaterEntryApplied() {
        super.awaitLaterEntryApplied();
        // The delayed entry itself lands ahead of the barrier
        saved.set(true);
      }
    };
    final UnresolvedConfigurationChanges changes = held(environment);

    changes.tryResolve(A_5);

    assertThat(environment.restored.get()).isZero();
  }
}
