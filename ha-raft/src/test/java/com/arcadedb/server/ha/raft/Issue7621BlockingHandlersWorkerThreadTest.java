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

import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.DeleteApiTokenHandler;
import com.arcadedb.server.http.handler.DeleteGroupHandler;
import com.arcadedb.server.http.handler.PostCommitHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Regression test for issue #7621: a handler whose {@code execute()} can block on a Raft round-trip or a
 * retried compare-and-set must declare {@code mustExecuteOnWorkerThread() == true}, or it stalls every other
 * connection on the same Undertow selector - including the kubelet readiness/liveness probes - while it
 * waits (issue #7133).
 * <p>
 * #7133 fixed three cluster-admin handlers by name; #7621 found three more with the identical shape that the
 * sweep had missed: {@link PostCommitHandler} (waits for commit publication), {@link DeleteGroupHandler} and
 * {@link DeleteApiTokenHandler} (both a Raft submit-and-wait with compare-and-set retries). Rather than trust
 * the next sweep to be exhaustive, this test enumerates every handler known to reach such a wait - both the
 * three #7133 fixed and the three #7621 fixes - and asserts each one overrides the method directly, so
 * removing the override (accidentally, in a refactor) fails the build instead of waiting to be found by hand
 * again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7621BlockingHandlersWorkerThreadTest {

  private static final Class<?>[] HANDLERS_THAT_MUST_RUN_ON_A_WORKER_THREAD = {
      // Fixed by issue #7133
      PostStepDownHandler.class,
      DeletePeerHandler.class,
      PostLeaveHandler.class,
      // Fixed by issue #7621
      PostCommitHandler.class,
      DeleteGroupHandler.class,
      DeleteApiTokenHandler.class,
  };

  @Test
  void everyKnownBlockingHandlerDeclaresItsOwnMustExecuteOnWorkerThread() {
    assertAll((Executable[]) java.util.Arrays.stream(HANDLERS_THAT_MUST_RUN_ON_A_WORKER_THREAD)
        .<Executable>map(handlerClass -> () -> assertThat(declaresMustExecuteOnWorkerThread(handlerClass))
            .as("%s must declare its own mustExecuteOnWorkerThread() override, not rely on the %s default of false",
                handlerClass.getSimpleName(), AbstractServerHttpHandler.class.getSimpleName())
            .isTrue())
        .toArray(Executable[]::new));
  }

  /**
   * True when {@code handlerClass} itself - not an ancestor - declares {@code mustExecuteOnWorkerThread()}.
   */
  private static boolean declaresMustExecuteOnWorkerThread(final Class<?> handlerClass) {
    try {
      final Method method = handlerClass.getDeclaredMethod("mustExecuteOnWorkerThread");
      return method.getReturnType() == boolean.class;
    } catch (final NoSuchMethodException e) {
      return false;
    }
  }
}
