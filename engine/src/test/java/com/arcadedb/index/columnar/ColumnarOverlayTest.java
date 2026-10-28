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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarOverlayTest {
  private static final int                MRP     = SegmentRoundTripTest.MAX_RECORDS_IN_PAGE;
  private static final ColumnDefinition[] COLUMNS = SegmentRoundTripTest.ALL; // brand, tags, size, score

  private final AtomicLong       clock   = new AtomicLong(1000);
  private final ColumnarOverlay  overlay = new ColumnarOverlay(MRP, COLUMNS, clock::get);
  private final RowBuffer        buffer  = new RowBuffer(MRP, COLUMNS);
  private final RowValues        values  = new RowValues(COLUMNS.length);

  private static long pos(final int page, final int slot) {
    return (long) page * MRP + slot;
  }

  /** A block whose rows carry brand {@code brandBase + slot}, tags {slot % 3, slot % 3 + 5}, size slot * 10 - 7, score slot + 0.5. */
  private OverlayPageBlock block(final int page, final long version, final int brandBase, final int... slots) {
    buffer.clear();
    for (final int slot : slots) {
      fill(brandBase + slot, slot);
      buffer.put(slot, values);
    }
    return OverlayPageBlock.build(page, MRP, COLUMNS, version, buffer);
  }

  private void fill(final int brand, final int slot) {
    values.present[0] = true;
    values.ordinal[0] = brand;
    values.present[1] = true;
    values.list[1] = new int[] { slot % 3, slot % 3 + 5 };
    values.listCount[1] = 2;
    values.present[2] = true;
    values.longs[2] = slot * 10L - 7;
    values.present[3] = true;
    values.doubles[3] = slot + 0.5;
  }

  private static List<Long> positions(final PositionBitmap bitmap) {
    final List<Long> result = new ArrayList<>();
    bitmap.forEach(result::add);
    return result;
  }

  @Test
  void replacePageSupersedesPreviousRows() {
    overlay.replacePage(block(3, 1, 100, 5, 9));
    overlay.replacePage(block(3, 2, 200, 5));

    final ColumnarSegmentView view = overlay.publish();
    assertThat(view.column("brand").ordinal(pos(3, 5))).isEqualTo(205);
    // the second block is the page's whole overlay content: slot 9 is no longer stored
    assertThat(view.column("brand").isPresent(pos(3, 9))).isFalse();
    assertThat(view.column("brand").ordinal(pos(3, 9))).isEqualTo(-1);
    assertThat(overlay.rows()).isEqualTo(1);
    assertThat(overlay.page(3).pageVersion()).isEqualTo(2);
  }

  @Test
  void blockCopiesTheStagedRows() {
    final OverlayPageBlock built = block(1, 1, 10, 4);
    // the list array handed to put() is written in place, then the buffer is refilled: the block keeps its own copy
    values.list[1][0] = 0;
    values.ordinal[0] = 99;
    buffer.clear();
    buffer.put(4, values);
    overlay.replacePage(built);

    final ColumnReader tags = overlay.publish().column("tags");
    assertThat(overlay.publish().column("brand").ordinal(pos(1, 4))).isEqualTo(14);
    final int[] out = new int[4];
    assertThat(tags.ordinals(pos(1, 4), out)).isEqualTo(2);
    assertThat(out[0]).isEqualTo(1);
    assertThat(out[1]).isEqualTo(6);

    // put() copies the list: writing the caller's array between put() and build() changes nothing
    buffer.clear();
    fill(0, 4);
    buffer.put(4, values);
    values.list[1][1] = 9;
    overlay.replacePage(OverlayPageBlock.build(2, MRP, COLUMNS, 1, buffer));
    assertThat(overlay.publish().column("tags").ordinals(pos(2, 4), out)).isEqualTo(2);
    assertThat(out[1]).isEqualTo(6);
  }

  @Test
  void pointReadsOfEveryKind() {
    buffer.clear();
    fill(42, 6);
    buffer.put(6, values);
    values.present[0] = false;
    values.present[1] = false;
    values.present[2] = false;
    values.present[3] = false;
    buffer.put(8, values);
    overlay.replacePage(OverlayPageBlock.build(2, MRP, COLUMNS, 3, buffer));

    final ColumnarSegmentView view = overlay.publish();
    assertThat(view.column("brand").kind()).isEqualTo(ColumnKind.ORDINAL);
    assertThat(view.column("size").longValue(pos(2, 6))).isEqualTo(53);
    assertThat(view.column("score").doubleValue(pos(2, 6))).isEqualTo(6.5);
    final int[] shortOut = new int[1];
    assertThat(view.column("tags").ordinals(pos(2, 6), shortOut)).isEqualTo(2);
    assertThat(shortOut[0]).isEqualTo(0);

    // a stored row with every value missing is live, and every column answers missing
    assertThat(view.live().get(pos(2, 8))).isTrue();
    assertThat(view.column("brand").isPresent(pos(2, 8))).isFalse();
    assertThat(view.column("size").longValue(pos(2, 8))).isZero();
    assertThat(view.column("score").doubleValue(pos(2, 8))).isZero();
    assertThat(view.column("tags").ordinals(pos(2, 8), shortOut)).isZero();
    // a position the view does not hold
    assertThat(view.column("size").isPresent(pos(2, 7))).isFalse();
    assertThat(view.column("size").isPresent(pos(5, 6))).isFalse();
    assertThat(view.column("nope")).isNull();
    assertThatThrownBy(() -> view.column("size").ordinal(pos(2, 6))).isInstanceOf(IllegalStateException.class);

    assertThat(view.minLong("size")).isEqualTo(53);
    assertThat(view.maxLong("size")).isEqualTo(53);
    assertThat(view.minLong("tags")).isEqualTo(0);
    assertThat(view.maxLong("tags")).isEqualTo(5);
    assertThat(Double.longBitsToDouble(view.maxLong("score"))).isEqualTo(6.5);
    assertThatThrownBy(() -> view.minLong("nope")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void minAndMaxSpanEveryBlock() {
    overlay.replacePage(block(1, 1, 0, 3));
    overlay.replacePage(block(4, 1, 0, 900));
    final ColumnarSegmentView view = overlay.publish();
    assertThat(view.minLong("size")).isEqualTo(23);
    assertThat(view.maxLong("size")).isEqualTo(8993);
    assertThat(view.minLong("brand")).isEqualTo(3);
    assertThat(view.maxLong("brand")).isEqualTo(900);
    assertThat(Double.longBitsToDouble(view.minLong("score"))).isEqualTo(3.5);

    final ColumnarSegmentView empty = new ColumnarOverlay(MRP, COLUMNS, clock::get).publish();
    assertThat(empty.minLong("size")).isEqualTo(Long.MAX_VALUE);
    assertThat(empty.maxLong("score")).isEqualTo(Long.MIN_VALUE);
    assertThat(empty.live().isEmpty()).isTrue();
    assertThat(empty.live().maxRecordsInPage()).isEqualTo(MRP);
  }

  @Test
  void publishedViewIsImmutable() {
    overlay.replacePage(block(3, 1, 100, 5));
    overlay.replacePage(block(6, 1, 100, 1));
    final ColumnarSegmentView v = overlay.publish();

    overlay.replacePage(block(3, 2, 300, 5));
    overlay.replacePage(block(4, 2, 300, 0));
    overlay.replacePage(OverlayPageBlock.build(6, MRP, COLUMNS, 2, null));

    assertThat(v.column("brand").ordinal(pos(3, 5))).isEqualTo(105);
    assertThat(v.column("brand").ordinal(pos(6, 1))).isEqualTo(101);
    assertThat(v.column("brand").isPresent(pos(4, 0))).isFalse();
    assertThat(positions(v.live())).containsExactly(pos(3, 5), pos(6, 1));

    final ColumnarSegmentView now = overlay.publish();
    assertThat(now.column("brand").ordinal(pos(3, 5))).isEqualTo(305);
    assertThat(positions(now.live())).containsExactly(pos(3, 5), pos(4, 0));
    assertThat(overlay.page(6)).isNull();
  }

  @Test
  void freezeHandsContentToFlushAndStartsEmpty() {
    final OverlayPageBlock one = block(1, 1, 0, 2, 3);
    final OverlayPageBlock two = block(2, 1, 0, 4);
    overlay.replacePage(two);
    overlay.replacePage(one);

    final ColumnarOverlay frozen = overlay.freeze();
    assertThat(frozen.rows()).isEqualTo(3);
    assertThat(frozen.pages()).containsExactly(1, 2);
    assertThat(frozen.page(1)).isSameAs(one);
    assertThat(frozen.page(2)).isSameAs(two);

    assertThat(overlay.rows()).isZero();
    assertThat(overlay.pages()).isEmpty();
    assertThat(overlay.page(1)).isNull();
    assertThat(overlay.publish().live().isEmpty()).isTrue();

    // the two overlays are independent from now on
    overlay.replacePage(block(1, 2, 50, 7));
    assertThat(frozen.page(1)).isSameAs(one);
    assertThat(frozen.publish().column("brand").isPresent(pos(1, 7))).isFalse();
    assertThat(overlay.publish().column("brand").isPresent(pos(1, 2))).isFalse();
  }

  @Test
  void liveIsExactlyTheStoredSlots() {
    overlay.replacePage(block(9, 1, 0, 1));
    overlay.replacePage(block(0, 1, 0, 0, 63, 64, 2047));

    final ColumnarSegmentView view = overlay.publish();
    assertThat(positions(view.live())).containsExactly(pos(0, 0), pos(0, 63), pos(0, 64), pos(0, 2047), pos(9, 1));
    assertThat(view.live().cardinality()).isEqualTo(overlay.rows()).isEqualTo(5);
    assertThat(view.live()).isSameAs(view.live());
    assertThat(positions(view.live(1, 10))).containsExactly(pos(9, 1));
    assertThat(positions(view.live(0, 9))).containsExactly(pos(0, 0), pos(0, 63), pos(0, 64), pos(0, 2047));
    assertThat(view.live(10, 20).isEmpty()).isTrue();

    // an empty block removes the page
    overlay.replacePage(OverlayPageBlock.build(0, MRP, COLUMNS, 2, null));
    assertThat(positions(overlay.publish().live())).containsExactly(pos(9, 1));
    assertThat(overlay.rows()).isEqualTo(1);
  }

  @Test
  void overlayForEachOrdinalIsAscending() {
    overlay.replacePage(block(7, 1, 0, 30, 2));
    overlay.replacePage(block(2, 1, 0, 2047, 0));
    // slot 9 of page 5 has no brand: never visited
    buffer.clear();
    fill(500, 1);
    buffer.put(1, values);
    values.present[0] = false;
    values.present[1] = false;
    buffer.put(9, values);
    overlay.replacePage(OverlayPageBlock.build(5, MRP, COLUMNS, 1, buffer));

    final PositionBitmap matches = new PositionBitmap(MRP);
    for (final long p : new long[] { pos(7, 30), pos(7, 2), pos(2, 2047), pos(2, 0), pos(5, 1), pos(5, 9), pos(5, 10), pos(3, 0) })
      matches.set(p);

    final ColumnarSegmentView view = overlay.publish();
    final List<Long> visited = new ArrayList<>();
    final List<Integer> ordinals = new ArrayList<>();
    view.column("brand").forEachOrdinal(matches, (position, ordinal) -> {
      visited.add(position);
      ordinals.add(ordinal);
    });
    assertThat(visited).containsExactly(pos(2, 0), pos(2, 2047), pos(5, 1), pos(7, 2), pos(7, 30));
    assertThat(ordinals).containsExactly(0, 2047, 500, 2, 30);

    // only matched positions: page 7 slot 30 left out
    matches.clear(pos(7, 30));
    final List<String> tagVisits = new ArrayList<>();
    view.column("tags").forEachOrdinal(matches, (position, ordinal) -> tagVisits.add(position + ":" + ordinal));
    assertThat(tagVisits).containsExactly(pos(2, 0) + ":0", pos(2, 0) + ":5", pos(2, 2047) + ":1", pos(2, 2047) + ":6",
        pos(5, 1) + ":1", pos(5, 1) + ":6", pos(7, 2) + ":2", pos(7, 2) + ":7");

    assertThatThrownBy(() -> view.column("size").forEachOrdinal(matches, (p, o) -> {
    })).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> view.column("brand").forEachOrdinal(new PositionBitmap(256), (p, o) -> {
    })).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void oldestRowAgeRestartsAtFreeze() {
    assertThat(overlay.oldestRowMs()).isZero();
    clock.set(1000);
    overlay.replacePage(block(1, 1, 0, 1));
    clock.set(1500);
    overlay.replacePage(block(2, 1, 0, 1));
    assertThat(overlay.oldestRowMs()).isEqualTo(1000);

    final ColumnarOverlay frozen = overlay.freeze();
    assertThat(frozen.oldestRowMs()).isEqualTo(1000);
    assertThat(overlay.oldestRowMs()).isZero();

    // an empty block on an empty overlay stores no row and starts no age
    clock.set(1800);
    overlay.replacePage(OverlayPageBlock.build(3, MRP, COLUMNS, 1, null));
    assertThat(overlay.oldestRowMs()).isZero();

    clock.set(2000);
    overlay.replacePage(block(1, 2, 0, 1));
    clock.set(2600);
    overlay.replacePage(block(1, 3, 0, 2));
    assertThat(overlay.oldestRowMs()).isEqualTo(2000);

    // emptied by replacement: no row left, no age
    overlay.replacePage(OverlayPageBlock.build(1, MRP, COLUMNS, 4, null));
    assertThat(overlay.oldestRowMs()).isZero();
  }

  @Test
  void publishedLiveSharesTheStripes() {
    final OverlayPageBlock three = block(3, 1, 0, 1, 70);
    final OverlayPageBlock eight = block(8, 1, 0, 5);
    overlay.replacePage(eight);
    overlay.replacePage(three);

    final PositionBitmap live = overlay.publish().live();
    assertThat(live.stripeForTesting(3)).isSameAs(three.liveStripe());
    assertThat(live.stripeForTesting(8)).isSameAs(eight.liveStripe());
  }

  @Test
  void writingAWrappedBitmapClonesTheStripeFirst() {
    final OverlayPageBlock three = block(3, 1, 0, 1, 70);
    final long[] stripe = three.liveStripe();
    final long[] before = stripe.clone();

    final PositionBitmap wrapped = PositionBitmap.wrapShared(MRP, new int[] { 3 }, new long[][] { stripe }, 1);
    wrapped.set(pos(3, 2));
    wrapped.clear(pos(3, 70));
    assertThat(stripe).isEqualTo(before);
    assertThat(positions(wrapped)).containsExactly(pos(3, 1), pos(3, 2));

    final PositionBitmap other = PositionBitmap.wrapShared(MRP, new int[] { 3 }, new long[][] { stripe }, 1);
    final PositionBitmap mask = new PositionBitmap(MRP);
    mask.set(pos(3, 1));
    other.andNot(mask);
    assertThat(stripe).isEqualTo(before);
    assertThat(positions(other)).containsExactly(pos(3, 70));
  }

  @Test
  void blockOfAnotherShapeIsRefused() {
    final ColumnDefinition[] fewer = { COLUMNS[0] };
    final RowBuffer small = new RowBuffer(256, fewer);
    assertThatThrownBy(() -> overlay.replacePage(OverlayPageBlock.build(1, 256, fewer, 1, small)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> OverlayPageBlock.build(1, MRP, COLUMNS, 1, small)).isInstanceOf(IllegalArgumentException.class);
    values.present[0] = true;
    values.ordinal[0] = -3;
    assertThatThrownBy(() -> buffer.put(1, values)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> buffer.put(MRP, values)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void frozenRowsWriteTheSameSegment(@TempDir final Path dir) throws IOException {
    overlay.replacePage(block(4, 40, 0, 2047, 1));
    buffer.clear();
    fill(9, 3);
    values.present[2] = false;
    buffer.put(3, values);
    overlay.replacePage(OverlayPageBlock.build(1, MRP, COLUMNS, 10, buffer));
    final ColumnarOverlay frozen = overlay.freeze();

    // what a flush does: every frozen page ascending, every row of its block
    final Path file = dir.resolve(SegmentFormat.segmentFileName(1));
    try (final SegmentWriter writer = new SegmentWriter(file, 1, MRP, COLUMNS, 16)) {
      final RowValues row = new RowValues(COLUMNS.length);
      for (final int page : frozen.pages()) {
        final OverlayPageBlock pageBlock = frozen.page(page);
        for (int r = 0; r < pageBlock.rowCount(); r++) {
          pageBlock.readRow(r, row);
          writer.addRow(pageBlock.position(r), pageBlock.pageVersion(), row);
        }
      }
      writer.finish();
    }

    final ColumnarSegmentView view = frozen.publish();
    try (final SegmentReader segment = SegmentReader.open(file, null)) {
      assertThat(positions(segment.live())).containsExactlyElementsOf(positions(view.live())).containsExactly(pos(1, 3), pos(4, 1),
          pos(4, 2047));
      assertThat(segment.pageVersion(1)).isEqualTo(10);
      assertThat(segment.pageVersion(4)).isEqualTo(40);
      final int[] out = new int[4];
      final int[] expected = new int[4];
      for (final long p : positions(view.live())) {
        assertThat(segment.column("brand").ordinal(p)).isEqualTo(view.column("brand").ordinal(p));
        assertThat(segment.column("size").isPresent(p)).isEqualTo(view.column("size").isPresent(p));
        assertThat(segment.column("size").longValue(p)).isEqualTo(view.column("size").longValue(p));
        assertThat(segment.column("score").doubleValue(p)).isEqualTo(view.column("score").doubleValue(p));
        assertThat(segment.column("tags").ordinals(p, out)).isEqualTo(view.column("tags").ordinals(p, expected));
        assertThat(out).isEqualTo(expected);
      }
      assertThat(segment.column("size").isPresent(pos(1, 3))).isFalse();
      for (final String property : new String[] { "brand", "tags", "size", "score" }) {
        assertThat(segment.minLong(property)).as(property).isEqualTo(view.minLong(property));
        assertThat(segment.maxLong(property)).as(property).isEqualTo(view.maxLong(property));
      }
    }
  }

  private record Published(int pass, ColumnarSegmentView view) {
  }

  @Test
  void concurrentReadersSeeOnlyWholePublishedPasses() throws Exception {
    final int pages = 8;
    final int passes = 2_000;
    final int readers = 4;
    final AtomicReference<Published> current = new AtomicReference<>(new Published(0, overlay.publish()));
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicInteger failures = new AtomicInteger();
    final AtomicLong checkedViews = new AtomicLong();
    final CyclicBarrier start = new CyclicBarrier(readers + 1);
    final CountDownLatch done = new CountDownLatch(readers);
    final PositionBitmap all = new PositionBitmap(MRP);
    for (int page = 0; page < pages; page++)
      all.setPage(page);

    for (int r = 0; r < readers; r++)
      new Thread(() -> {
        try {
          start.await();
          while (writing.get() || checkedViews.get() == 0) {
            final Published published = current.get();
            final int pass = published.pass();
            // pass p stores slots 0..p % 5 + 1 of every page, brand p * 4096 + slot: anything else is a torn view
            final long expectedRows = pass == 0 ? 0 : (long) pages * (pass % 5 + 2);
            final long[] seen = { 0 };
            published.view().column("brand").forEachOrdinal(all, (position, ordinal) -> {
              if (ordinal != pass * 4096 + (int) (position % MRP))
                failures.incrementAndGet();
              seen[0]++;
            });
            if (seen[0] != expectedRows || published.view().live().cardinality() != expectedRows)
              failures.incrementAndGet();
            checkedViews.incrementAndGet();
          }
        } catch (final Throwable e) {
          failures.incrementAndGet();
        } finally {
          done.countDown();
        }
      }).start();

    start.await();
    for (int pass = 1; pass <= passes; pass++) {
      final int[] slots = new int[pass % 5 + 2];
      for (int i = 0; i < slots.length; i++)
        slots[i] = i;
      for (int page = 0; page < pages; page++)
        overlay.replacePage(block(page, pass, pass * 4096, slots));
      current.set(new Published(pass, overlay.publish()));
    }
    writing.set(false);
    done.await();
    assertThat(failures.get()).isZero();
    assertThat(checkedViews.get()).isPositive();
  }
}
