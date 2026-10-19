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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10140: a request admitted on one thread whose statement runs on another - a gRPC call inside a client-managed
 * transaction runs on the transaction's own thread - carries its slot there, so {@link QueryAdmissionGate#releaseCurrentSlot()}
 * called by the statement (a forward to the leader, a wait on an explicit lock) gives it back.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class QueryAdmissionGateHandoffTest {
  // A FRESH GATE PER TEST: THE COUNTERS OF THE JVM-WIDE ONE ARE SHARED WITH EVERY OTHER TEST
  private final QueryAdmissionGate gate     = new QueryAdmissionGate();
  private final ExecutorService    executor = Executors.newSingleThreadExecutor();

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
  }

  @Test
  void withoutTheHandoffTheOtherThreadCannotGiveTheSlotBack() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      // THE BUG THE HAND-OFF FIXES: THE THREAD THAT RUNS THE STATEMENT HOLDS NO SLOT, SO THE RELEASE IS A SILENT NO-OP
      assertThat(onOtherThread(gate::releaseCurrentSlot)).isFalse();
      assertThat(gate.getRunning()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void theThreadThatAdoptsTheSlotGivesItBackOnceAndRestoresWhatItHeld() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final QueryAdmissionGate.Ticket ticket = gate.admit();
    final QueryAdmissionGate.Handoff handoff = gate.handOff();

    final boolean released = onOtherThread(() -> {
      try (final QueryAdmissionGate.Adoption ignored = handoff.adopt()) {
        // A QUERY THE STATEMENT STARTS SHARES THE CARRIED SLOT INSTEAD OF QUEUEING FOR THE ONLY ONE, WHICH IT HOLDS
        try (final QueryAdmissionGate.Ticket nested = gate.admit()) {
          assertThat(gate.getRunning()).isEqualTo(1);
        }
        // THE STATEMENT IS ABOUT TO WAIT ON THE LEADER OR ON A LOCK
        return gate.releaseCurrentSlot();
      }
    });
    assertThat(released).isTrue();
    assertThat(gate.getRunning()).isZero();

    // THE ADOPTION IS OVER: THE THREAD HOLDS NOTHING, AND ITS NEXT QUERY TAKES A SLOT OF ITS OWN
    assertThat(onOtherThread(gate::releaseCurrentSlot)).isFalse();
    assertThat(onOtherThread(() -> {
      try (final QueryAdmissionGate.Ticket own = gate.admit()) {
        return gate.getRunning();
      }
    })).isEqualTo(1);

    // THE TICKET OF THE REQUEST CLOSES NORMALLY AND GIVES NOTHING BACK A SECOND TIME
    ticket.close();
    assertThat(gate.getRunning()).isZero();
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getRunning()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aSlotAlreadyGivenBackIsNotCarried() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);

    final QueryAdmissionGate.Ticket ticket = gate.admit();
    final QueryAdmissionGate.Handoff handoff = gate.handOff();
    ticket.close();
    assertThat(gate.getRunning()).isZero();

    // THE TICKET WAS CLOSED BEFORE THE STATEMENT RAN: NOTHING TO PUBLISH, NOTHING TO GIVE BACK A SECOND TIME
    assertThat(onOtherThread(() -> {
      try (final QueryAdmissionGate.Adoption ignored = handoff.adopt()) {
        return gate.releaseCurrentSlot();
      }
    })).isFalse();
    assertThat(gate.getRunning()).isZero();

    // NOTHING HELD ON THIS THREAD EITHER: A HAND-OFF CARRIES NOTHING
    assertThat(onOtherThread(() -> {
      try (final QueryAdmissionGate.Adoption ignored = gate.handOff().adopt()) {
        return gate.releaseCurrentSlot();
      }
    })).isFalse();
  }

  @Test
  void aDisabledGateCarriesNothing() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(0);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final QueryAdmissionGate.Handoff handoff = gate.handOff();
      assertThat(onOtherThread(() -> {
        try (final QueryAdmissionGate.Adoption adoption = handoff.adopt()) {
          return gate.releaseCurrentSlot();
        }
      })).isFalse();
    }
    assertThat(gate.getRunning()).isZero();
  }

  private <T> T onOtherThread(final Callable<T> body) throws Exception {
    return executor.submit(body).get(30, TimeUnit.SECONDS);
  }
}
