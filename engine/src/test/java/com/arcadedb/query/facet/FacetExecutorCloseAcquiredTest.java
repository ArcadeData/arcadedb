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
package com.arcadedb.query.facet;

import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.PositionBitmap;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Final review A-M5: closing the snapshots the executor acquired never loses the failure the execution already ends with; a close
 * failure is added to it as suppressed, and every snapshot is closed whatever the others do.
 */
class FacetExecutorCloseAcquiredTest {

  /** A snapshot whose close throws when asked to, and remembers that it was closed. */
  private static final class ClosingSnapshot implements ColumnarSnapshot {
    private final boolean throwing;
    private       boolean closed;

    private ClosingSnapshot(final boolean throwing) {
      this.throwing = throwing;
    }

    @Override
    public int bucketId() {
      return 0;
    }

    @Override
    public List<ColumnarSegmentView> segments() {
      return List.of();
    }

    @Override
    public PositionBitmap pendingPositions() {
      return new PositionBitmap(16);
    }

    @Override
    public long oldestPendingEpochStartMs() {
      return 0;
    }

    @Override
    public boolean isUsable() {
      return true;
    }

    @Override
    public GlobalDictionary dictionary(final String property) {
      return null;
    }

    @Override
    public boolean publishes(final String property) {
      return false;
    }

    @Override
    public ColumnDefinition publishedColumn(final String property) {
      return null;
    }

    @Override
    public void close() {
      closed = true;
      if (throwing)
        throw new IllegalStateException("close failed");
    }
  }

  @Test
  void aCloseFailureIsSuppressedIntoTheFailureInFlight() {
    final ClosingSnapshot first = new ClosingSnapshot(true);
    final ClosingSnapshot healthy = new ClosingSnapshot(false);
    final ClosingSnapshot last = new ClosingSnapshot(true);
    final RuntimeException inFlight = new RuntimeException("counting failed");

    FacetExecutor.closeAcquired(new ColumnarSnapshot[] { first, healthy, last }, new boolean[] { true, true, true }, inFlight);

    assertThat(first.closed && healthy.closed && last.closed).as("every snapshot closed").isTrue();
    assertThat(inFlight.getSuppressed()).hasSize(2).allMatch(e -> "close failed".equals(e.getMessage()));
  }

  @Test
  void withoutAFailureInFlightTheFirstCloseFailureIsThrown() {
    final ClosingSnapshot first = new ClosingSnapshot(true);
    final ClosingSnapshot notAcquired = new ClosingSnapshot(true);
    final ClosingSnapshot last = new ClosingSnapshot(true);

    final Throwable thrown = catchThrowable(() -> FacetExecutor.closeAcquired(new ColumnarSnapshot[] { first, notAcquired, last },
        new boolean[] { true, false, true }, null));

    assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("close failed");
    assertThat(thrown.getSuppressed()).hasSize(1);
    assertThat(notAcquired.closed).as("a snapshot the match set owns is not closed here").isFalse();
  }
}
