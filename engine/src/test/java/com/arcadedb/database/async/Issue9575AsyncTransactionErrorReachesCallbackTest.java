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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Found on PR #10121 (issue #9575): an {@link Error} thrown inside an async transaction, an {@link AssertionError} or an
 * {@link OutOfMemoryError} on a large load, was caught by the worker loop and rolled back, but never reached the task's own
 * error callback. A caller that waits for completion and then asks its callback what failed - the parallel flush of a
 * GraphBatch - took the task for one that had committed, and left the edges of that bucket unconnected without a word.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575AsyncTransactionErrorReachesCallbackTest extends TestHelper {

  @Test
  void anErrorInAnAsyncTransactionReachesItsErrorCallbackAndRollsBack() {
    final AtomicBoolean okCalled = new AtomicBoolean();
    final AtomicReference<Throwable> taskError = new AtomicReference<>();

    database.async().transaction(() -> {
          database.newDocument("Doc").set("id", 1).save();
          throw new AssertionError("simulated");
        }, 0, () -> okCalled.set(true), taskError::set);
    database.async().waitCompletion();

    assertThat(okCalled.get()).isFalse();
    assertThat(taskError.get()).isInstanceOf(AssertionError.class).hasMessage("simulated");
    assertThat(database.countType("Doc", false)).as("documents left by the failed transaction").isZero();
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }
}
