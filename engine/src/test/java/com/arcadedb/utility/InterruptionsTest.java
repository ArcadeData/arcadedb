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
package com.arcadedb.utility;

import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@link Interruptions} tells apart: a read that an interrupt stopped (issue #12349) from a read that failed. The test
 * thread's interrupt flag is part of the input, so every test leaves it cleared.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class InterruptionsTest {

  @AfterEach
  void clearInterruptFlag() {
    Thread.interrupted();
  }

  @Test
  void aFailureOnAThreadThatIsNotInterruptedIsNotAnInterruption() {
    assertThat(Interruptions.isInterruption(new IllegalStateException("damaged"))).isFalse();
    assertThat(Interruptions.isInterruption(new RuntimeException("wrapper", new IOException("disk")))).isFalse();
  }

  @Test
  void anyFailureOnAnInterruptedThreadIsAnInterruption() {
    Thread.currentThread().interrupt();
    assertThat(Interruptions.isInterruption(new IllegalStateException("whatever it was"))).isTrue();
  }

  @Test
  void theInterruptExceptionsOfAReadAreRecognisedWhereverTheyAreInTheCauseChain() {
    assertThat(Interruptions.isInterruption(new InterruptedIOException("page lock"))).isTrue();
    assertThat(Interruptions.isInterruption(new ClosedByInterruptException())).isTrue();
    assertThat(Interruptions.isInterruption(new InterruptedException("sleep"))).isTrue();

    // LocalBucket wraps the InterruptedIOException of the page lock, and a caller may wrap it again
    final Throwable wrapped = new DatabaseOperationException("Error on reading record",
        new RuntimeException("again", new InterruptedIOException("page lock")));
    assertThat(Interruptions.isInterruption(wrapped)).isTrue();
  }

  @Test
  @Timeout(30)
  void aCauseChainThatLoopsBackOnItselfEndsWithoutAnInterruption() {
    final RuntimeException first = new RuntimeException("first");
    final RuntimeException second = new RuntimeException("second");
    first.initCause(second);
    second.initCause(first);

    assertThat(Interruptions.isInterruption(first)).isFalse();
  }

  @Test
  void rethrowIfInterruptionReturnsForAFailureThatIsNotAnInterruption() {
    assertThatCode(() -> Interruptions.rethrowIfInterruption(new IllegalStateException("damaged"))).doesNotThrowAnyException();
    assertThatCode(() -> Interruptions.rethrowIfInterruption(new IOException("disk"))).doesNotThrowAnyException();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  void anUncheckedInterruptionIsRethrownAsItIs() {
    final RuntimeException wrapped = new DatabaseOperationException("Error on reading record",
        new InterruptedIOException("page lock"));

    assertThatThrownBy(() -> Interruptions.rethrowIfInterruption(wrapped)).isSameAs(wrapped);
  }

  @Test
  void anUncheckedFailureOnAnInterruptedThreadIsRethrownAsItIs() {
    final RuntimeException failure = new IllegalStateException("damaged");
    Thread.currentThread().interrupt();

    assertThatThrownBy(() -> Interruptions.rethrowIfInterruption(failure)).isSameAs(failure);
  }

  @Test
  void aCheckedInterruptionIsWrappedInAnUncheckedOne() {
    final InterruptedIOException interruption = new InterruptedIOException("page lock");

    assertThatThrownBy(() -> Interruptions.rethrowIfInterruption(interruption)).isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("interrupted").hasCause(interruption);
  }

  @Test
  void theInterruptFlagIsStillSetAfterAnInterruptionThatClearedItIsRethrown() {
    // AN InterruptedException CLEARS THE FLAG AS IT IS THROWN: THE CANCELLATION MUST STAY VISIBLE TO THE CALLERS OF THE READ
    assertThat(Thread.currentThread().isInterrupted()).isFalse();

    assertThatThrownBy(() -> Interruptions.rethrowIfInterruption(new InterruptedException("slept"))).isInstanceOf(
        DatabaseOperationException.class);

    assertThat(Thread.interrupted()).as("the interrupt flag survives the rethrow").isTrue();
  }

  @Test
  void theInterruptFlagIsNotSetByAFailureThatIsNotAnInterruption() {
    Interruptions.rethrowIfInterruption(new IllegalStateException("damaged"));

    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }
}
