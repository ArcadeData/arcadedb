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
package com.arcadedb.server.support;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The loop that registers a keyed server as an installation without Studio: once after a delay, again after a failure with a
 * growing wait, never when disabled or unregistered, never again after a refused key, and never an exception out of it. The
 * delays are shrunk to milliseconds through {@link SupportAutoRegistration.Timing}.
 */
class SupportAutoRegistrationTest {
  private static final SupportAutoRegistration.Timing FAST = new SupportAutoRegistration.Timing(20L, new long[] { 20L, 40L, 80L }, 60_000L);

  private SupportAutoRegistration registration;

  @AfterEach
  void stop() {
    if (registration != null)
      registration.close();
  }

  private SupportAutoRegistration start(final SupportAutoRegistration.Timing timing, final AtomicBoolean enabled,
      final AtomicBoolean registered, final Supplier<String> action) {
    registration = new SupportAutoRegistration(timing, enabled::get, registered::get, action::get, () -> 0L, t -> {
    });
    registration.start();
    return registration;
  }

  @Test
  void registersOnceAfterTheDelayAndThenWaitsForTheNextDay() {
    final AtomicInteger calls = new AtomicInteger();
    start(FAST, new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      calls.incrementAndGet();
      return "{}";
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() == 1);
    sleep(300);
    assertThat(calls.get()).as("the next attempt is a day away").isEqualTo(1);
  }

  @Test
  void repeatsEveryIntervalWhileTheServerRuns() {
    final AtomicInteger calls = new AtomicInteger();
    start(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 50L), new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      calls.incrementAndGet();
      return "{}";
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() >= 3);
  }

  @Test
  void retriesWithBackoffWhenThePortalIsUnreachable() {
    final AtomicInteger calls = new AtomicInteger();
    start(FAST, new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      if (calls.incrementAndGet() < 3)
        throw new SupportPortalException("portal_unreachable", 0, "The portal cannot be reached", 0);
      return "{}";
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() == 3);
    sleep(300);
    assertThat(calls.get()).as("after a success it waits for the next interval").isEqualTo(3);
  }

  @Test
  void anUnexpectedFailureIsRetriedToo() {
    final AtomicInteger calls = new AtomicInteger();
    start(FAST, new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      if (calls.incrementAndGet() == 1)
        throw new IllegalStateException("diagnostics not ready");
      return "{}";
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() == 2);
  }

  @Test
  void doesNothingWhenDisabled() {
    final AtomicInteger calls = new AtomicInteger();
    start(FAST, new AtomicBoolean(false), new AtomicBoolean(true), () -> {
      calls.incrementAndGet();
      return "{}";
    });
    sleep(300);
    assertThat(calls.get()).isZero();
  }

  @Test
  void doesNothingWhenTheServerIsNotRegistered() {
    final AtomicInteger calls = new AtomicInteger();
    start(FAST, new AtomicBoolean(true), new AtomicBoolean(false), () -> {
      calls.incrementAndGet();
      return "{}";
    });
    sleep(300);
    assertThat(calls.get()).isZero();
  }

  @Test
  void stopsForTheRunWhenTheKeyIsRefused() {
    final AtomicInteger calls = new AtomicInteger();
    start(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 20L), new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      calls.incrementAndGet();
      throw new SupportPortalException("invalid_key", 401, "The key is not valid", 0);
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() == 1);
    sleep(300);
    assertThat(calls.get()).as("no retry after invalid_key, however short the interval").isEqualTo(1);
  }

  @Test
  void stopsOnADefinitiveRefusalOfThePortal() {
    final AtomicInteger calls = new AtomicInteger();
    start(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 20L), new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      calls.incrementAndGet();
      throw new SupportPortalException("instance_id.taken", 500, "this instance id is already registered", 0);
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() == 1);
    sleep(300);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aLapsedPlanIsTriedAgainAtTheNextInterval() {
    final AtomicInteger calls = new AtomicInteger();
    start(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 50L), new AtomicBoolean(true), new AtomicBoolean(true), () -> {
      if (calls.incrementAndGet() == 1)
        throw new SupportPortalException("support_not_active", 402, "plan lapsed", 0);
      return "{}";
    });
    await().atMost(java.time.Duration.ofSeconds(5)).until(() -> calls.get() >= 2);
  }

  @Test
  void aRegistrationDoneRecentlyElsewhereIsNotRepeated() {
    // Studio (or the connect flow) registered 10 seconds ago: the next automatic attempt waits out the rest of the interval
    final AtomicInteger calls = new AtomicInteger();
    final AtomicBoolean on = new AtomicBoolean(true);
    registration = new SupportAutoRegistration(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 60_000L), on::get,
        () -> true, () -> {
          calls.incrementAndGet();
          return "{}";
        }, () -> System.currentTimeMillis() - 10_000L, t -> {
        });
    registration.start();
    sleep(300);
    assertThat(calls.get()).isZero();
  }

  @Test
  void closeStopsTheThreadAndStartIsIdempotent() {
    final AtomicInteger calls = new AtomicInteger();
    final SupportAutoRegistration r = start(new SupportAutoRegistration.Timing(10L, new long[] { 10L }, 20L), new AtomicBoolean(true),
        new AtomicBoolean(true), () -> {
          calls.incrementAndGet();
          return "{}";
        });
    r.start();
    r.close();
    final int atClose = calls.get();
    sleep(200);
    assertThat(calls.get()).isLessThanOrEqualTo(atClose + 1);
    assertThat(r.isRunning()).isFalse();
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
