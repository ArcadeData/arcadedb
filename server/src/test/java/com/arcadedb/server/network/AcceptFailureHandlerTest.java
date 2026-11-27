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
package com.arcadedb.server.network;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What an accept loop does with a failure of the work of one accepted connection: an error pauses it and is logged at a
 * bounded rate, an exception is only logged, and neither ends the loop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AcceptFailureHandlerTest {

  @Test
  void anErrorPausesTheLoop() {
    final AcceptFailureHandler handler = new AcceptFailureHandler("TEST");

    final long start = System.nanoTime();
    handler.handle(this, "Error on client connection", new OutOfMemoryError("start failed"));
    final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // a lower bound only: a stall can make the pause longer, never shorter
    assertThat(elapsedMs).isGreaterThanOrEqualTo(AcceptFailureHandler.PAUSE_MS - 10);
  }

  @Test
  void anExceptionIsNotHeldBack() {
    final AcceptFailureHandler handler = new AcceptFailureHandler("TEST");

    for (int i = 0; i < 3; i++)
      handler.handle(this, "Error on client connection", new IllegalStateException("unable to start the connection"));

    // an exception keeps being logged as before: the bounded log is the one of errors
    assertThat(handler.getHeldBack()).isZero();
  }

  @Test
  void errorsAreLoggedAtABoundedRate() {
    final AcceptFailureHandler handler = new AcceptFailureHandler("TEST");

    for (int i = 0; i < 3; i++)
      handler.handle(this, "Error on client connection", new OutOfMemoryError("start failed"));

    // the first is logged, the two that follow inside the interval are held back and counted
    assertThat(handler.getHeldBack()).isEqualTo(2);
  }

  @Test
  void anInterruptedPauseKeepsTheInterruption() {
    final AcceptFailureHandler handler = new AcceptFailureHandler("TEST");

    Thread.currentThread().interrupt();
    try {
      handler.handle(this, "Error on client connection", new OutOfMemoryError("start failed"));
      assertThat(Thread.currentThread().isInterrupted()).as("the interruption is not swallowed").isTrue();
    } finally {
      Thread.interrupted();
    }
  }
}
