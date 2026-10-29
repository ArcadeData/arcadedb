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

package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CombinedLoadTest {
  private static FakeLoad quiet(final int ackedWrites) {
    final Ledger ledger = new Ledger(1);
    final FakeLoad load = new FakeLoad(ledger);
    for (int i = 0; i < ackedWrites; i++)
      ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    load.progress = false;
    return load;
  }

  @Test
  void fansOutAndReportsThePrimary() {
    final FakeLoad primary = quiet(3);
    final FakeLoad other = quiet(8);
    final CombinedLoad combined = new CombinedLoad(primary, other);
    combined.start();
    try {
      combined.quiesce();
    } catch (final InterruptedException e) {
      throw new AssertionError(e);
    }
    assertThat(combined.acked()).isEqualTo(primary.acked()).isEqualTo(3L);
    combined.resume();
    combined.close();
    assertThat(primary.quiesced).isEqualTo(1);
    assertThat(other.quiesced).isEqualTo(1);
    assertThat(primary.resumed).isEqualTo(1);
    assertThat(other.resumed).isEqualTo(1);
    assertThat(primary.closed).isTrue();
    assertThat(other.closed).isTrue();
  }

  @Test
  void closeReachesEveryMemberWhenOneFails() {
    final FakeLoad last = quiet(0);
    final LoadGenerator failing = new LoadGenerator() {
      @Override
      public void start() {
      }

      @Override
      public void quiesce() {
      }

      @Override
      public void resume() {
      }

      @Override
      public long acked() {
        return 0;
      }

      @Override
      public void close() {
        throw new IllegalStateException("boom");
      }
    };
    final FakeLoad first = quiet(0);
    final CombinedLoad combined = new CombinedLoad(first, failing, last);
    assertThatThrownBy(combined::close).isInstanceOf(IllegalStateException.class).hasMessage("boom");
    assertThat(first.closed).isTrue();
    assertThat(last.closed).isTrue();
    combined.close();
  }
}
