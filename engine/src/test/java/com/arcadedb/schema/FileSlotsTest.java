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
package com.arcadedb.schema;

import com.arcadedb.engine.Component;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8634: file-id lookups must not take a lock, and a whole-table publication must never be
 * observed half written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FileSlotsTest {

  @Test
  void growsAndResolvesById() {
    final FileSlots slots = new FileSlots();
    assertThat(slots.get(0)).isNull();
    assertThat(slots.get(-1)).isNull();
    slots.set(40, null);
    assertThat(slots.size()).isEqualTo(41);
    slots.add(null);
    assertThat(slots.size()).isEqualTo(42);
    assertThat(slots.get(1000)).isNull();
    slots.clear();
    assertThat(slots.size()).isEqualTo(0);
  }

  @Test
  void replaceAllPublishesTheWholeTable() {
    final FileSlots slots = new FileSlots();
    slots.set(3, null);
    slots.replaceAll(new Component[5]);
    assertThat(slots.size()).isEqualTo(5);
    assertThat(slots.toArray()).hasSize(5);
    assertThat(slots.toList()).hasSize(5);
  }

  @Test
  void readersAreNotBlockedByAHeldWriterMonitor() throws Exception {
    final FileSlots slots = new FileSlots();
    final AtomicBoolean done = new AtomicBoolean();
    final Thread reader = new Thread(() -> {
      for (int i = 0; i < 1000; i++)
        slots.get(i);
      done.set(true);
    });
    synchronized (slots) {
      reader.start();
      reader.join(10_000);
      assertThat(done.get()).as("a reader must not wait for the monitor a writer holds").isTrue();
    }
  }

  @Test
  void concurrentGrowthNeverLosesOrTearsSlots() throws Exception {
    final FileSlots slots = new FileSlots();
    final AtomicInteger errors = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final Thread[] readers = new Thread[4];
    for (int t = 0; t < readers.length; t++) {
      readers[t] = new Thread(() -> {
        while (!stop.get()) {
          final int size = slots.size();
          if (slots.toArray().length < Math.min(size, 0))
            errors.incrementAndGet();
          slots.get(size);
        }
      });
      readers[t].start();
    }
    for (int i = 0; i < 20_000; i++)
      slots.add(null);
    stop.set(true);
    for (final Thread r : readers)
      r.join(10_000);
    assertThat(errors.get()).isZero();
    assertThat(slots.size()).isEqualTo(20_000);
  }
}
