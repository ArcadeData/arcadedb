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
package com.arcadedb.index.columnar;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Test fixture over {@link ColumnarTestHooks#killPoint}: the first thread that reaches the named point blocks until
 * {@link #releaseAsCrashed()}, then throws {@link ColumnarTestHooks.SimulatedCrash}, so the operation stops exactly there. The point
 * fires once: a later reach (for example the recovery of a reopened database while the point is still armed) passes through. Every
 * other point name passes through. Call {@link #disarm()} in {@code @AfterEach}.
 */
public final class KillPoint {
  private final String         name;
  private final CountDownLatch reached  = new CountDownLatch(1);
  private final CountDownLatch released = new CountDownLatch(1);
  private final AtomicBoolean  fired    = new AtomicBoolean();
  private final Consumer<String> hook   = this::onReach;

  private KillPoint(final String name) {
    this.name = name;
  }

  /**
   * Installs a kill point on the named point.
   *
   * @throws IllegalStateException when another kill point is armed
   */
  public static KillPoint arm(final String name) {
    if (ColumnarTestHooks.killPoint != null)
      throw new IllegalStateException("A kill point is already armed");
    final KillPoint point = new KillPoint(name);
    ColumnarTestHooks.killPoint = point.hook;
    return point;
  }

  /** Waits until a thread reached the point; false on timeout. */
  public boolean awaitReached(final long timeoutMs) throws InterruptedException {
    return reached.await(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /** Lets the blocked thread go on, throwing {@link ColumnarTestHooks.SimulatedCrash} from the point. */
  public void releaseAsCrashed() {
    released.countDown();
  }

  /** Removes the hook when it is still this one, and releases a blocked thread as crashed, so a failed test never hangs a thread. */
  public void disarm() {
    if (ColumnarTestHooks.killPoint == hook)
      ColumnarTestHooks.killPoint = null;
    released.countDown();
  }

  private void onReach(final String point) {
    if (!name.equals(point) || !fired.compareAndSet(false, true))
      return;
    reached.countDown();
    try {
      released.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    throw new ColumnarTestHooks.SimulatedCrash();
  }
}
