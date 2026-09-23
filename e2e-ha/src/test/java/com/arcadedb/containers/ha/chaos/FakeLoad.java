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

/**
 * Load generator that, while {@code progress} is set, acknowledges one new write each time {@link #acked()} is read,
 * which is how the runner observes progress.
 */
final class FakeLoad implements LoadGenerator {
  private final Ledger  ledger;
  boolean progress = true;
  int     quiesced;
  int     resumed;
  boolean closed;

  FakeLoad(final Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public void start() {
  }

  @Override
  public void quiesce() {
    ++quiesced;
  }

  @Override
  public void resume() {
    ++resumed;
  }

  @Override
  public long acked() {
    if (progress && !closed)
      ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    return ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE);
  }

  @Override
  public void close() {
    closed = true;
  }
}
