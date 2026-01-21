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
package com.arcadedb.server.ha;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for circuit breaker state machine that prevents cascading failures.
 */
public class ReplicaCircuitBreakerTest {

  @Test
  public void testStartsInClosedState() {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 30000L, "test-replica");

    // Then
    assertEquals(ReplicaCircuitBreaker.State.CLOSED, breaker.getState());
    assertTrue(breaker.shouldAttempt(), "Should allow messages in CLOSED state");
  }

  @Test
  public void testOpensAfterThresholdFailures() {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 30000L, "test-replica");

    // When: Record failures up to threshold
    for (int i = 0; i < 4; i++) {
      breaker.recordFailure();
      assertEquals(ReplicaCircuitBreaker.State.CLOSED, breaker.getState(),
          "Should stay CLOSED before threshold");
    }

    // When: Hit threshold
    breaker.recordFailure();

    // Then
    assertEquals(ReplicaCircuitBreaker.State.OPEN, breaker.getState(),
        "Should transition to OPEN after threshold");
    assertFalse(breaker.shouldAttempt(), "Should reject messages in OPEN state");
  }

  @Test
  public void testSuccessResetsFailureCount() {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 30000L, "test-replica");

    // When: Record some failures then success
    breaker.recordFailure();
    breaker.recordFailure();
    assertEquals(2, breaker.getFailureCount(), "Should have 2 failures");

    breaker.recordSuccess();

    // Then
    assertEquals(0, breaker.getFailureCount(), "Should reset failure count on success");
    assertEquals(ReplicaCircuitBreaker.State.CLOSED, breaker.getState(),
        "Should remain CLOSED after success");
  }

  @Test
  public void testHalfOpenAfterTimeout() throws InterruptedException {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 100L, "test-replica");

    // When: Open the circuit
    for (int i = 0; i < 5; i++) {
      breaker.recordFailure();
    }
    assertEquals(ReplicaCircuitBreaker.State.OPEN, breaker.getState());
    assertFalse(breaker.shouldAttempt(), "Should reject immediately after opening");

    // When: Wait for timeout
    Thread.sleep(150);

    // Then
    assertTrue(breaker.shouldAttempt(), "Should allow probe after timeout");
    assertEquals(ReplicaCircuitBreaker.State.HALF_OPEN, breaker.getState(),
        "Should transition to HALF_OPEN after timeout");
  }

  @Test
  public void testHalfOpenClosesAfterSuccesses() {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 30000L, "test-replica");
    breaker.forceHalfOpen(); // Helper method for testing

    // When: Record successes up to threshold
    for (int i = 0; i < 2; i++) {
      breaker.recordSuccess();
      assertEquals(ReplicaCircuitBreaker.State.HALF_OPEN, breaker.getState(),
          "Should stay HALF_OPEN before success threshold");
    }

    // When: Hit success threshold
    breaker.recordSuccess();

    // Then
    assertEquals(ReplicaCircuitBreaker.State.CLOSED, breaker.getState(),
        "Should transition to CLOSED after success threshold");
    assertEquals(0, breaker.getFailureCount(), "Should reset failure count");
  }

  @Test
  public void testHalfOpenReopensOnFailure() {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(5, 3, 30000L, "test-replica");
    breaker.forceHalfOpen(); // Helper method for testing

    // When: Record one success then a failure
    breaker.recordSuccess();
    assertEquals(ReplicaCircuitBreaker.State.HALF_OPEN, breaker.getState());

    breaker.recordFailure();

    // Then
    assertEquals(ReplicaCircuitBreaker.State.OPEN, breaker.getState(),
        "Should transition back to OPEN on failure");
    assertFalse(breaker.shouldAttempt(), "Should reject messages after reopening");
  }

  @Test
  public void testThreadSafety() throws InterruptedException {
    // Given
    final ReplicaCircuitBreaker breaker = new ReplicaCircuitBreaker(100, 10, 30000L, "test-replica");
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final Thread[] threads = new Thread[threadCount];

    // When: Multiple threads record successes/failures concurrently
    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      threads[i] = new Thread(() -> {
        for (int j = 0; j < operationsPerThread; j++) {
          if (threadIndex % 2 == 0) {
            breaker.recordSuccess();
          } else {
            breaker.recordFailure();
          }
          breaker.shouldAttempt();
        }
      });
      threads[i].start();
    }

    // Wait for completion
    for (Thread thread : threads) {
      thread.join();
    }

    // Then: No exceptions and state is valid
    assertNotNull(breaker.getState());
    assertTrue(breaker.getFailureCount() >= 0, "Failure count should be non-negative");
  }
}
