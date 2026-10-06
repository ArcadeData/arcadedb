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
package com.arcadedb.database;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * State that survives the attempts of one retried {@link Database#transaction} call, unlike a
 * {@link TransactionContext#setAttachment transaction attachment}, which a rollback drops together with the attempt.
 * <p>
 * It exists for a side effect that cannot be rolled back (the Redis INCR/GETDEL on the shared RAM map, issue #9322): the
 * first attempt performs it and parks what it took in a slot, a retry that re-runs the same statements finds the slot
 * and answers from it instead of performing the effect again. Slots are handed out in call order and the order restarts
 * with every attempt, so the n-th request of a retry meets the slot the n-th request of the first attempt filled. That
 * holds for a block that re-runs deterministically, which is what a retry is.
 * <p>
 * The scope belongs to the outermost {@code transaction()} call of the thread and ends with it, whether it commits or
 * gives up. It is reachable through {@link DatabaseContext.DatabaseContextTL#getRetryScope()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RetryScope {
  private final List<Object> slots = new ArrayList<>(2);
  private       int          next;

  /**
   * @return the slot at the current position, created by {@code factory} the first time the position is reached, then
   * advances the position
   */
  @SuppressWarnings("unchecked")
  public <T> T nextSlot(final Supplier<T> factory) {
    final int index = next++;
    if (index == slots.size())
      slots.add(factory.get());
    return (T) slots.get(index);
  }

  /** Called when an attempt starts over: its requests meet the slots the previous attempt filled, from the first. */
  void restartAttempt() {
    next = 0;
  }
}
