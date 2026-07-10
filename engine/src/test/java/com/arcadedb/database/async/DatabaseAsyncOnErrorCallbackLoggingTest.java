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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #4553: when a user-supplied {@code onError} callback itself throws, the
 * exception thrown by the callback (the new, otherwise-silent failure) must be the one logged,
 * not the original async error that the caller already knows about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseAsyncOnErrorCallbackLoggingTest extends TestHelper {

  @Test
  void callbackFailureIsLoggedInsteadOfOriginalError() {
    final DatabaseAsyncExecutor async = database.async();

    final RuntimeException originalError = new RuntimeException("original async error");
    final RuntimeException callbackError = new RuntimeException("failure inside onError callback");

    // The user-supplied callback blows up while handling the async error.
    async.onError(t -> {
      throw callbackError;
    });

    final AtomicReference<Throwable> loggedThrowable = new AtomicReference<>();
    try {
      LogManager.instance().setLogger(new CapturingLogger(loggedThrowable));

      async.onError(originalError);
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }

    // Must log the callback's own exception (e1), not the already-known original error (e).
    assertThat(loggedThrowable.get()).isSameAs(callbackError);
    assertThat(loggedThrowable.get()).isNotSameAs(originalError);
  }

  /**
   * Minimal {@link Logger} that records the {@link Throwable} passed to the first SEVERE log call.
   */
  private static class CapturingLogger implements Logger {
    private final AtomicReference<Throwable> captured;

    private CapturingLogger(final AtomicReference<Throwable> captured) {
      this.captured = captured;
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context, final Object arg1,
        final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      if (iLevel == Level.SEVERE && iException != null)
        captured.compareAndSet(null, iException);
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context, final Object... args) {
      if (iLevel == Level.SEVERE && iException != null)
        captured.compareAndSet(null, iException);
    }

    @Override
    public void flush() {
    }
  }
}
