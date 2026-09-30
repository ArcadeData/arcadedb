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

  private static final class Stub extends Component {
    Stub(final int id) {
      super(null, "c" + id, id, 0, "");
    }

    @Override
    public void close() {
    }
  }

  @Test
  void growsAndResolvesById() {
    final FileSlots slots = new FileSlots();
    assertThat(slots.get(0)).isNull();
    assertThat(slots.get(-1)).isNull();
    final Stub c40 = new Stub(40);
    slots.set(40, c40);
    assertThat(slots.size()).isEqualTo(41);
    assertThat(slots.get(40)).isSameAs(c40);
    assertThat(slots.get(39)).isNull();
    final Stub c41 = new Stub(41);
    slots.add(c41);
    assertThat(slots.size()).isEqualTo(42);
    assertThat(slots.get(41)).isSameAs(c41);
    assertThat(slots.get(1000)).isNull();
    assertThat(slots.findByName("c41")).isSameAs(c41);
    assertThat(slots.findByName("nope")).isNull();
    slots.clear();
    assertThat(slots.size()).isEqualTo(0);
    assertThat(slots.get(40)).isNull();
  }

  @Test
  void replaceAllPublishesTheWholeTable() {
    final FileSlots slots = new FileSlots();
    slots.set(3, new Stub(3));
    final Stub replacement = new Stub(1);
    slots.replaceAll(new Component[] { null, replacement, null, null, null });
    assertThat(slots.size()).isEqualTo(5);
    assertThat(slots.get(3)).isNull();
    assertThat(slots.get(1)).isSameAs(replacement);
    assertThat(slots.toArray()).hasSize(5);
    assertThat(slots.toList()).hasSize(5).contains(replacement);
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

  /** A reader that sees a size must find every slot below it filled, however the table grew meanwhile. */
  @Test
  void concurrentGrowthNeverExposesAnUnfilledSlot() throws Exception {
    final int total = 20_000;
    final FileSlots slots = new FileSlots();
    final AtomicInteger errors = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final Thread[] readers = new Thread[4];
    for (int t = 0; t < readers.length; t++) {
      readers[t] = new Thread(() -> {
        while (!stop.get()) {
          final Component[] copy = slots.toArray();
          for (int i = 0; i < copy.length; i++)
            if (copy[i] == null || copy[i].getFileId() != i) {
              errors.incrementAndGet();
              break;
            }
          if (copy.length > 0 && slots.findByName("c" + (copy.length - 1)) == null)
            errors.incrementAndGet();
        }
      });
      readers[t].start();
    }
    for (int i = 0; i < total; i++)
      slots.add(new Stub(i));
    stop.set(true);
    for (final Thread r : readers)
      r.join(10_000);
    assertThat(errors.get()).isZero();
    assertThat(slots.size()).isEqualTo(total);
  }
}
