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
import java.util.Objects;
import java.util.function.Supplier;

/**
 * State that survives the attempts of one retried {@link Database#transaction} call, unlike a {@link
 * TransactionContext#setAttachment transaction attachment}, which a rollback drops together with the attempt.
 * <p>
 * It exists for a side effect that cannot be rolled back (the Redis INCR/GETDEL on the shared RAM map, issue #9322):
 * the first attempt performs it and parks what it took in a slot, a retry that re-runs the same statements finds the
 * slot and answers from it instead of performing the effect again. Slots are handed out in call order and the order
 * restarts with every attempt, so the n-th request of a retry meets the slot the n-th request of the first attempt
 * filled, provided it carries the same key. A retry that diverges (a data-dependent branch) gets fresh slots from the
 * first position whose key changed on, instead of the wrong ones; the positions before it keep theirs. Caveat: the effect
 * of the replaced position is applied a second time (an INCR that ran in the lost attempt stays applied), so the guarantee
 * of exactly-once is only as good as the determinism of the retried block.
 * <p>
 * Not thread-safe: it is reached through the thread context. The scope belongs to the outermost {@code transaction()} call
 * of the thread on that database (one scope per database, as the thread context is) and ends with it, whether it commits
 * or gives up. It is reachable through {@link DatabaseContext.DatabaseContextTL#getRetryScope()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RetryScope {
  private final List<Slot> slots = new ArrayList<>(2);
  private       int        next;

  private static final class Slot {
    private final Object key;
    private       Object value;

    private Slot(final Object key, final Object value) {
      this.key = key;
      this.value = value;
    }
  }

  /**
   * @param key     what the slot was made for (the statement text): a retry that takes a different path and reaches this
   *                position with another key does not inherit the slot of the statement that was there before, it gets a
   *                fresh one, so a mismatch costs a re-applied effect at worst and never a wrong answer
   * @param factory creates the slot value
   * @return the slot at the current position, created by {@code factory} the first time the position is reached with this
   * key, then advances the position
   */
  @SuppressWarnings("unchecked")
  public <T> T nextSlot(final Object key, final Supplier<T> factory) {
    final int index = next++;
    if (index == slots.size()) {
      final Slot slot = new Slot(key, factory.get());
      slots.add(slot);
      return (T) slot.value;
    }
    final Slot slot = slots.get(index);
    if (!Objects.equals(slot.key, key)) {
      // The retry diverged here: nothing after this position is trusted either, even where a later key happens to match
      slots.subList(index, slots.size()).clear();
      final Slot fresh = new Slot(key, factory.get());
      slots.add(fresh);
      return (T) fresh.value;
    }
    return (T) slot.value;
  }

  int cursor() {
    return next;
  }

  /** Called when an attempt starts over: its requests meet the slots the previous attempt filled, from where it began. */
  void restoreCursor(final int cursor) {
    next = cursor;
  }
}
