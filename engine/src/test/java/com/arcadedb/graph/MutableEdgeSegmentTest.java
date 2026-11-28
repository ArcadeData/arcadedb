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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of the raw entry codec of {@link MutableEdgeSegment}: every operation is exercised on a segment held in memory and
 * checked against the entries it must hold, byte-exact where the contract is about room (an entry that fits to the last byte
 * is taken, one byte short is refused and leaves the segment untouched).
 */
class MutableEdgeSegmentTest extends TestHelper {
  private static final int HEADER = MutableEdgeSegment.CONTENT_START_POSITION;

  /** The entry {@code {edgeBucket, edgePosition, vertexBucket, vertexPosition}}. */
  private static long[] entry(final long edgeBucket, final long edgePosition, final long vertexBucket, final long vertexPosition) {
    return new long[] { edgeBucket, edgePosition, vertexBucket, vertexPosition };
  }

  private static int sizeOf(final long[] entry) {
    return Binary.getNumberSpace(entry[0]) + Binary.getNumberSpace(entry[1]) + Binary.getNumberSpace(entry[2]) + Binary.getNumberSpace(
        entry[3]);
  }

  private MutableEdgeSegment segment(final int bytes) {
    return new MutableEdgeSegment((DatabaseInternal) database, bytes);
  }

  private MutableEdgeSegment segmentHolding(final long[]... entries) {
    int bytes = HEADER;
    for (final long[] e : entries)
      bytes += sizeOf(e);
    final MutableEdgeSegment segment = segment(bytes);
    for (final long[] e : entries)
      assertThat(segment.addAtEndDirect((int) e[0], e[1], (int) e[2], e[3])).isTrue();
    return segment;
  }

  private static List<long[]> contentOf(final MutableEdgeSegment segment) {
    final List<long[]> entries = new ArrayList<>();
    segment.forEachEntry((eb, ep, vb, vp) -> entries.add(entry(eb, ep, vb, vp)));
    return entries;
  }

  private static void assertEntries(final MutableEdgeSegment segment, final long[]... expected) {
    final List<long[]> found = contentOf(segment);
    assertThat(found).hasSize(expected.length);
    for (int i = 0; i < expected.length; i++)
      assertThat(found.get(i)).as("entry %d", i).containsExactly(expected[i]);
    int bytes = HEADER;
    for (final long[] e : expected)
      bytes += sizeOf(e);
    assertThat(segment.getUsed()).as("used bytes").isEqualTo(bytes);
    assertThat(segment.isEmpty()).isEqualTo(expected.length == 0);
  }

  private static RID rid(final long bucket, final long position) {
    return new RID((int) bucket, position);
  }

  @Test
  void aFreshSegmentIsEmptyAndHasNoPrevious() {
    final MutableEdgeSegment segment = segment(64);

    assertThat(segment.getUsed()).isEqualTo(HEADER);
    assertThat(segment.isEmpty()).isTrue();
    assertThat(segment.getPreviousRID()).isNull();
    assertThat(segment.count(null)).isZero();
    assertThat(segment.containsEdge(rid(1, 1))).isFalse();
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(1, 1), null)).isNull();
    assertThat(segment.removeEdge(rid(1, 1))).isZero();
    assertThat(segment.removeVertex(rid(1, 1))).isZero();
    assertThat(segment.removeLightEdge(1, rid(1, 1))).isZero();
    assertThat(segment.containsLightEdge(1, rid(1, 1))).isFalse();
    assertThat(segment.countLightEdges(1, rid(1, 1))).isZero();
    assertThat(contentOf(segment)).isEmpty();
  }

  @Test
  void addPutsTheNewestFirstAndTakesAnEntryThatFitsToTheLastByte() {
    final long[] first = entry(3, 5, 4, 7);
    final long[] second = entry(3, 6, 4, 8);
    final long[] third = entry(3, 7, 4, 9);
    final MutableEdgeSegment segment = segment(HEADER + sizeOf(first) + sizeOf(second) + sizeOf(third));

    assertThat(segment.add(rid(3, 5), rid(4, 7))).isTrue();
    assertThat(segment.add(rid(3, 6), rid(4, 8))).isTrue();
    // THE LAST ENTRY FILLS THE SEGMENT EXACTLY: IT IS STILL ROOM
    assertThat(segment.add(rid(3, 7), rid(4, 9))).isTrue();
    assertEntries(segment, third, second, first);

    // FULL: REFUSED AND UNTOUCHED
    assertThat(segment.add(rid(3, 8), rid(4, 10))).isFalse();
    assertEntries(segment, third, second, first);
  }

  @Test
  void addRefusesAnEntryOneByteLargerThanTheRoomLeft() {
    final long[] small = entry(3, 5, 4, 7);
    // A POSITION OF 64 NEEDS TWO VLQ BYTES (ZIGZAG 128), SO THE ENTRY IS ONE BYTE LARGER THAN THE ROOM LEFT
    final long[] wide = entry(3, 64, 4, 7);
    assertThat(sizeOf(wide)).isEqualTo(sizeOf(small) + 1);

    final MutableEdgeSegment segment = segment(HEADER + sizeOf(small) + sizeOf(small));
    assertThat(segment.add(rid(3, 5), rid(4, 7))).isTrue();
    assertThat(segment.add(rid(3, 64), rid(4, 7))).isFalse();
    assertEntries(segment, small);
    assertThat(segment.add(rid(3, 6), rid(4, 7))).isTrue();
    assertEntries(segment, entry(3, 6, 4, 7), small);
  }

  @Test
  void addAtEndDirectKeepsInsertionOrderAndUsesTheRoomExactly() {
    final long[] a = entry(200, 5, 300, 70);
    final long[] b = entry(201, 6, 301, 71);
    final long[] c = entry(3, 7, 4, 9);
    final MutableEdgeSegment segment = segment(HEADER + sizeOf(a) + sizeOf(b) + sizeOf(c));

    assertThat(segment.addAtEndDirect(200, 5, 300, 70)).isTrue();
    assertThat(segment.addAtEndDirect(201, 6, 301, 71)).isTrue();
    // THE SEGMENT HAS EXACTLY THE ROOM OF THE THIRD ENTRY
    assertThat(segment.addAtEndDirect(3, 7, 4, 9)).isTrue();
    assertEntries(segment, a, b, c);

    assertThat(segment.addAtEndDirect(3, 7, 4, 9)).isFalse();
    assertEntries(segment, a, b, c);
  }

  @Test
  void addAtEndDirectRefusesOneByteTooMany() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] wide = entry(3, 64, 4, 7);
    final MutableEdgeSegment segment = segment(HEADER + sizeOf(a) + sizeOf(wide) - 1);

    assertThat(segment.addAtEndDirect(3, 5, 4, 7)).isTrue();
    assertThat(segment.addAtEndDirect(3, 64, 4, 7)).isFalse();
    assertEntries(segment, a);
    // THE REFUSED ENTRY DID NOT LEAVE A TRACE: A SMALLER ONE TAKES ITS PLACE
    assertThat(segment.addAtEndDirect(3, 6, 4, 7)).isTrue();
    assertEntries(segment, a, entry(3, 6, 4, 7));
  }

  @Test
  void addManyAtEndDirectWritesTheRangeThatFitsAndReportsHowMany() {
    final int[] eb = { 3, 3, 3, 3, 3 };
    final long[] ep = { 10, 11, 12, 13, 14 };
    final int[] vb = { 4, 4, 4, 4, 4 };
    final long[] vp = { 20, 21, 22, 23, 24 };
    final int each = sizeOf(entry(3, 10, 4, 20));

    // ROOM FOR THREE OF THE FIVE, TAKEN FROM THE SECOND ONE ON: THE FIRST ONE IS NOT PART OF THE RANGE
    final MutableEdgeSegment segment = segment(HEADER + 3 * each);
    assertThat(segment.addManyAtEndDirect(eb, ep, vb, vp, 1, 5)).isEqualTo(3);
    assertEntries(segment, entry(3, 11, 4, 21), entry(3, 12, 4, 22), entry(3, 13, 4, 23));

    // NOTHING FITS ANY MORE: NOTHING IS WRITTEN AND THE USED BYTES DO NOT MOVE
    final int used = segment.getUsed();
    assertThat(segment.addManyAtEndDirect(eb, ep, vb, vp, 0, 5)).isZero();
    assertThat(segment.getUsed()).isEqualTo(used);

    // AN EMPTY RANGE WRITES NOTHING
    final MutableEdgeSegment roomy = segment(HEADER + 5 * each);
    assertThat(roomy.addManyAtEndDirect(eb, ep, vb, vp, 2, 2)).isZero();
    assertEntries(roomy);

    // EVERYTHING FITS TO THE LAST BYTE
    assertThat(roomy.addManyAtEndDirect(eb, ep, vb, vp, 0, 5)).isEqualTo(5);
    assertEntries(roomy, entry(3, 10, 4, 20), entry(3, 11, 4, 21), entry(3, 12, 4, 22), entry(3, 13, 4, 23), entry(3, 14, 4, 24));
  }

  @Test
  void addManyAtEndDirectAppendsAfterTheEntriesThatAreThere() {
    final long[] existing = entry(3, 5, 4, 7);
    final MutableEdgeSegment segment = segment(HEADER + 3 * sizeOf(existing));
    assertThat(segment.addAtEndDirect(3, 5, 4, 7)).isTrue();

    assertThat(segment.addManyAtEndDirect(new int[] { 3, 3 }, new long[] { 6, 7 }, new int[] { 4, 4 }, new long[] { 8, 9 }, 0, 2)).isEqualTo(
        2);
    assertEntries(segment, existing, entry(3, 6, 4, 8), entry(3, 7, 4, 9));
  }

  @Test
  void addGrowingUpToGrowsTheChunkByWhatTheEntryNeedsAndNoFurtherThanTheCap() {
    final long[] first = entry(3, 5, 4, 7);
    final long[] second = entry(3, 6, 4, 8);
    final long[] third = entry(3, 7, 4, 9);
    final MutableEdgeSegment segment = segment(HEADER + sizeOf(first));
    assertThat(segment.add(rid(3, 5), rid(4, 7))).isTrue();

    // THE CHUNK IS FULL AND THE CAP IS ONE BYTE SHORT OF WHAT THE ENTRY NEEDS: REFUSED AND UNTOUCHED
    final int neededForSecond = HEADER + sizeOf(first) + sizeOf(second);
    assertThat(segment.addGrowingUpTo(rid(3, 6), rid(4, 8), neededForSecond - 1)).isFalse();
    assertEntries(segment, first);
    assertThat(segment.getRecordSize()).isEqualTo(HEADER + sizeOf(first));

    // THE CAP EQUALS WHAT IT NEEDS: THE CHUNK GROWS TO EXACTLY THAT
    assertThat(segment.addGrowingUpTo(rid(3, 6), rid(4, 8), neededForSecond)).isTrue();
    assertEntries(segment, second, first);
    assertThat(segment.getRecordSize()).isEqualTo(neededForSecond);

    // FULL AGAIN AT THE CAP
    assertThat(segment.addGrowingUpTo(rid(3, 7), rid(4, 9), neededForSecond)).isFalse();
    assertEntries(segment, second, first);

    // ROOM LEFT IN THE CHUNK: NO GROWTH
    final MutableEdgeSegment roomy = segment(HEADER + 3 * sizeOf(first));
    assertThat(roomy.addGrowingUpTo(rid(3, 5), rid(4, 7), HEADER)).isTrue();
    assertThat(roomy.addGrowingUpTo(rid(3, 6), rid(4, 8), HEADER)).isTrue();
    assertThat(roomy.addGrowingUpTo(rid(3, 7), rid(4, 9), HEADER)).isTrue();
    // THE CHUNK WAS NEVER GROWN: THE FOURTH DOES NOT FIT AND THE CAP FORBIDS GROWING
    assertThat(roomy.addGrowingUpTo(rid(3, 8), rid(4, 10), HEADER)).isFalse();
    assertEntries(roomy, third, second, first);

    // THE ENTRY PUT AFTER A GROWTH FITS THE GROWN CHUNK
    assertThat(segment.addGrowingUpTo(rid(3, 7), rid(4, 9), neededForSecond + sizeOf(third))).isTrue();
    assertEntries(segment, third, second, first);
  }

  @Test
  void containsEdgeAndFirstEdgeConnectedToVertexReadEveryEntry() {
    final MutableEdgeSegment segment = segmentHolding(entry(3, 5, 4, 7), entry(3, 6, 5, 8), entry(4, 7, 4, 7), entry(3, 9, 6, 10));

    assertThat(segment.containsEdge(rid(3, 5))).isTrue();
    assertThat(segment.containsEdge(rid(3, 6))).isTrue();
    assertThat(segment.containsEdge(rid(4, 7))).isTrue();
    assertThat(segment.containsEdge(rid(3, 9))).isTrue();
    // SAME POSITION IN ANOTHER BUCKET, SAME BUCKET AT ANOTHER POSITION, AND A VERTEX RID READ AS AN EDGE RID
    assertThat(segment.containsEdge(rid(4, 5))).isFalse();
    assertThat(segment.containsEdge(rid(3, 7))).isFalse();
    assertThat(segment.containsEdge(rid(4, 7 + 1))).isFalse();
    assertThat(segment.containsEdge(rid(5, 8))).isFalse();

    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 7), null)).isEqualTo(rid(3, 5));
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(5, 8), null)).isEqualTo(rid(3, 6));
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(6, 10), null)).isEqualTo(rid(3, 9));
    // THE VERTEX BUCKET AND THE VERTEX POSITION BOTH COUNT
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(5, 7), null)).isNull();
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 8), null)).isNull();
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(9, 9), null)).isNull();

    // THE FILTER NAMES THE EDGE BUCKETS ALLOWED: THE FIRST ENTRY ON THE VERTEX MAY BE FILTERED OUT AND A LATER ONE TAKEN
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 7), new int[] { 4 })).isEqualTo(rid(4, 7));
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 7), new int[] { 9, 3 })).isEqualTo(rid(3, 5));
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 7), new int[] { 9 })).isNull();
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(4, 7), new int[0])).isNull();
    assertThat(segment.getFirstEdgeConnectedToVertex(rid(5, 8), new int[] { 4 })).isNull();
  }

  @Test
  void removeEdgeTakesTheFirstEntryOfThatEdgeAndClosesTheGap() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(3, 6, 4, 8);
    final long[] c = entry(3, 7, 4, 9);
    final long[] d = entry(4, 7, 4, 10);

    // THE MIDDLE ONE
    MutableEdgeSegment segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeEdge(rid(3, 6))).isEqualTo(1);
    assertEntries(segment, a, c, d);
    assertThat(segment.containsEdge(rid(3, 6))).isFalse();

    // THE FIRST ONE
    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeEdge(rid(3, 5))).isEqualTo(1);
    assertEntries(segment, b, c, d);

    // THE LAST ONE: NOTHING TO MOVE
    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeEdge(rid(4, 7))).isEqualTo(1);
    assertEntries(segment, a, b, c);

    // THE ONLY ONE
    segment = segmentHolding(a);
    assertThat(segment.removeEdge(rid(3, 5))).isEqualTo(1);
    assertEntries(segment);

    // THE EDGE RID IS BUCKET AND POSITION: NEITHER ALONE IS A MATCH, AND A VERTEX RID DOES NOT COUNT
    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeEdge(rid(4, 6))).isZero();
    assertThat(segment.removeEdge(rid(3, 10))).isZero();
    assertThat(segment.removeEdge(rid(4, 8))).isZero();
    assertEntries(segment, a, b, c, d);
  }

  @Test
  void removeEdgeStopsAtTheFirstMatchAndKeepsTheEntriesAfterIt() {
    // THE SAME EDGE LISTED TWICE (A SELF LOOP IS IN BOTH LISTS, A DUPLICATE CAN BE FROM A REPAIR): ONE ENTRY GOES
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(3, 6, 4, 8);
    final MutableEdgeSegment segment = segmentHolding(a, b, a);

    assertThat(segment.removeEdge(rid(3, 5))).isEqualTo(1);
    assertEntries(segment, b, a);
    assertThat(segment.removeEdge(rid(3, 5))).isEqualTo(1);
    assertEntries(segment, b);
    assertThat(segment.removeEdge(rid(3, 5))).isZero();
  }

  @Test
  void removeVertexTakesTheFirstEntryReachingThatVertexAndClosesTheGap() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(3, 6, 5, 8);
    final long[] c = entry(3, 7, 4, 9);
    final long[] d = entry(3, 8, 6, 9);

    MutableEdgeSegment segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeVertex(rid(5, 8))).isEqualTo(1);
    assertEntries(segment, a, c, d);

    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeVertex(rid(4, 7))).isEqualTo(1);
    assertEntries(segment, b, c, d);

    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeVertex(rid(6, 9))).isEqualTo(1);
    assertEntries(segment, a, b, c);

    segment = segmentHolding(a);
    assertThat(segment.removeVertex(rid(4, 7))).isEqualTo(1);
    assertEntries(segment);

    // BUCKET AND POSITION OF THE VERTEX BOTH COUNT, AND AN EDGE RID DOES NOT
    segment = segmentHolding(a, b, c, d);
    assertThat(segment.removeVertex(rid(5, 7))).isZero();
    assertThat(segment.removeVertex(rid(4, 8))).isZero();
    assertThat(segment.removeVertex(rid(3, 5))).isZero();
    assertEntries(segment, a, b, c, d);

    // ONLY THE FIRST ENTRY OF A VERTEX LISTED TWICE
    segment = segmentHolding(a, b, c);
    assertThat(segment.removeVertex(rid(4, 7))).isEqualTo(1);
    assertEntries(segment, b, c);
  }

  @Test
  void lightEdgesAreMatchedByTypeBucketAndFarVertexAndNeverByARegularEdgeOfTheSameType() {
    // A LIGHT EDGE HAS NO RECORD: ITS POSITION IS NEGATIVE
    final long[] light = entry(3, -1, 4, 7);
    final long[] lightTwin = entry(3, -1, 4, 7);
    final long[] regularSameType = entry(3, 12, 4, 7);
    final long[] otherType = entry(5, -1, 4, 7);
    final long[] otherVertex = entry(3, -1, 4, 8);
    final long[] otherVertexBucket = entry(3, -1, 6, 7);

    final MutableEdgeSegment segment = segmentHolding(regularSameType, otherType, otherVertex, otherVertexBucket, light, lightTwin);

    assertThat(segment.containsLightEdge(3, rid(4, 7))).isTrue();
    assertThat(segment.containsLightEdge(5, rid(4, 7))).isTrue();
    assertThat(segment.containsLightEdge(3, rid(4, 8))).isTrue();
    assertThat(segment.containsLightEdge(3, rid(6, 7))).isTrue();
    assertThat(segment.containsLightEdge(4, rid(4, 7))).isFalse();
    assertThat(segment.containsLightEdge(3, rid(4, 9))).isFalse();
    assertThat(segment.containsLightEdge(3, rid(7, 7))).isFalse();

    assertThat(segment.countLightEdges(3, rid(4, 7))).isEqualTo(2);
    assertThat(segment.countLightEdges(5, rid(4, 7))).isEqualTo(1);
    assertThat(segment.countLightEdges(3, rid(4, 8))).isEqualTo(1);
    assertThat(segment.countLightEdges(3, rid(6, 7))).isEqualTo(1);
    assertThat(segment.countLightEdges(4, rid(4, 7))).isZero();
    assertThat(segment.countLightEdges(3, rid(4, 9))).isZero();
    assertThat(segment.countLightEdges(3, rid(7, 7))).isZero();

    // ONE ENTRY GOES PER CALL, AND NEVER THE REGULAR EDGE OF THE SAME TYPE REACHING THE SAME VERTEX
    assertThat(segment.removeLightEdge(3, rid(4, 7))).isEqualTo(1);
    assertEntries(segment, regularSameType, otherType, otherVertex, otherVertexBucket, lightTwin);
    assertThat(segment.removeLightEdge(3, rid(4, 7))).isEqualTo(1);
    assertEntries(segment, regularSameType, otherType, otherVertex, otherVertexBucket);
    assertThat(segment.removeLightEdge(3, rid(4, 7))).isZero();
    assertThat(segment.containsLightEdge(3, rid(4, 7))).isFalse();
    assertThat(segment.containsEdge(rid(3, 12))).isTrue();

    // THE FIRST, IN THE MIDDLE AND AT THE END OF WHAT IS LEFT
    assertThat(segment.removeLightEdge(5, rid(4, 7))).isEqualTo(1);
    assertEntries(segment, regularSameType, otherVertex, otherVertexBucket);
    assertThat(segment.removeLightEdge(3, rid(6, 7))).isEqualTo(1);
    assertEntries(segment, regularSameType, otherVertex);
    assertThat(segment.removeLightEdge(3, rid(4, 8))).isEqualTo(1);
    assertEntries(segment, regularSameType);
    assertThat(segment.removeLightEdge(3, rid(4, 7))).isZero();
  }

  @Test
  void removeEntryClosesTheGapBetweenTheTwoPositionsOfTheEntry() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(3, 6, 4, 8);
    final long[] c = entry(3, 7, 4, 9);
    final int size = sizeOf(a);

    // THE MIDDLE ENTRY
    MutableEdgeSegment segment = segmentHolding(a, b, c);
    assertThat(segment.removeEntry(HEADER + size, HEADER + 2 * size)).isTrue();
    assertEntries(segment, a, c);

    // THE LAST ENTRY: THE NEXT POSITION IS THE END OF THE USED BYTES
    segment = segmentHolding(a, b, c);
    assertThat(segment.removeEntry(HEADER + 2 * size, HEADER + 3 * size)).isTrue();
    assertEntries(segment, a, b);

    // THE FIRST ENTRY
    segment = segmentHolding(a, b, c);
    assertThat(segment.removeEntry(HEADER, HEADER + size)).isTrue();
    assertEntries(segment, b, c);

    // THE ONLY ENTRY
    segment = segmentHolding(a);
    assertThat(segment.removeEntry(HEADER, HEADER + size)).isTrue();
    assertEntries(segment);
  }

  @Test
  void removeEntryRefusesWhatIsNotInTheUsedBytes() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(3, 6, 4, 8);
    final int size = sizeOf(a);

    // AN EMPTY SEGMENT HAS NOTHING TO REMOVE
    final MutableEdgeSegment empty = segment(64);
    assertThat(empty.removeEntry(HEADER, HEADER + size)).isFalse();
    assertEntries(empty);

    // A START PAST THE USED BYTES
    final MutableEdgeSegment segment = segmentHolding(a, b);
    assertThat(segment.removeEntry(HEADER + 2 * size + 1, HEADER + 2 * size + 1 + size)).isFalse();
    assertEntries(segment, a, b);

    // AN END PAST THE USED BYTES IS A CORRUPTED BOUNDARY: THE SEGMENT IS CUT AT THE START OF THE ENTRY
    assertThat(segment.removeEntry(HEADER + size, HEADER + 2 * size + 1)).isFalse();
    assertEntries(segment, a);

    // A START EXACTLY AT THE USED BYTES IS NOT PAST THEM, AND THE END EXACTLY AT THEM IS NOT CORRUPTED
    final MutableEdgeSegment exact = segmentHolding(a, b);
    assertThat(exact.removeEntry(HEADER + 2 * size, HEADER + 2 * size)).isTrue();
    assertEntries(exact, a, b);
  }

  @Test
  void countAndCountIntoFilterByTheEdgeBucketAndTheFarVertexBucket() {
    final long self = 7;
    final MutableEdgeSegment segment = segmentHolding(entry(3, 5, 4, 7), entry(3, 6, 5, 8), entry(4, 7, 4, 7), entry(3, -1, 4, self),
        entry(5, 9, 6, 10));

    assertThat(segment.count(null)).isEqualTo(5);
    assertThat(segment.count(EdgeBucketMask.ofBucketIds(new int[] { 3 }))).isEqualTo(3);
    assertThat(segment.count(EdgeBucketMask.ofBucketIds(new int[] { 4, 5 }))).isEqualTo(2);
    assertThat(segment.count(EdgeBucketMask.ofBucketIds(new int[] { 9 }))).isZero();

    final EdgeBucketMask edge3 = EdgeBucketMask.ofBucketIds(new int[] { 3 });
    final EdgeBucketMask edge45 = EdgeBucketMask.ofBucketIds(new int[] { 4, 5 });
    final EdgeBucketMask vertex4 = EdgeBucketMask.ofBucketIds(new int[] { 4 });
    final EdgeBucketMask vertex56 = EdgeBucketMask.ofBucketIds(new int[] { 5, 6 });

    // EVERY FILTER IS ANSWERED IN ONE PASS, A NULL MASK ACCEPTS ANYTHING
    long[] counts = new long[5];
    segment.countInto(new EdgeBucketMask[] { null, edge3, edge45, edge3, null },
        new EdgeBucketMask[] { null, null, null, vertex4, vertex56 }, null, null, counts);
    assertThat(counts).containsExactly(5, 3, 2, 2, 2);

    // THE SELF LOOPS OF THE FILTERS THAT ASK FOR IT ARE LEFT OUT: THE OWNER IS #4:7, WHICH THREE ENTRIES REACH
    counts = new long[3];
    segment.countInto(new EdgeBucketMask[] { null, edge3, edge3 }, new EdgeBucketMask[] { null, null, null },
        new boolean[] { true, true, false }, rid(4, self), counts);
    assertThat(counts).containsExactly(2, 1, 3);

    // THE OWNER'S BUCKET AND POSITION BOTH IDENTIFY A SELF LOOP
    counts = new long[1];
    segment.countInto(new EdgeBucketMask[] { null }, new EdgeBucketMask[] { null }, new boolean[] { true }, rid(5, self), counts);
    assertThat(counts).containsExactly(5);
    counts = new long[1];
    segment.countInto(new EdgeBucketMask[] { null }, new EdgeBucketMask[] { null }, new boolean[] { true }, rid(4, 8), counts);
    assertThat(counts).containsExactly(5);

    // THE COUNTS ARE ADDED TO, NOT REPLACED
    counts = new long[] { 10 };
    segment.countInto(new EdgeBucketMask[] { edge3 }, new EdgeBucketMask[] { null }, null, null, counts);
    assertThat(counts).containsExactly(13);

    // A FLAG WITHOUT THE OWNER IS REFUSED, EVEN ON AN EMPTY SEGMENT
    final long[] untouched = new long[1];
    assertThatThrownBy(() -> segment(64).countInto(new EdgeBucketMask[] { null }, new EdgeBucketMask[] { null },
        new boolean[] { true }, null, untouched)).isInstanceOf(IllegalArgumentException.class);

    // AN EMPTY SEGMENT ADDS NOTHING
    segment(64).countInto(new EdgeBucketMask[] { null }, new EdgeBucketMask[] { null }, null, null, untouched);
    assertThat(untouched).containsExactly(0);
  }

  @Test
  void nextEntryInBucketsReturnsTheStartOfTheFirstEntryTheMaskAccepts() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(200, 6, 4, 8);
    final long[] c = entry(3, 7, 4, 9);
    final long[] d = entry(4, 7, 4, 10);
    final MutableEdgeSegment segment = segmentHolding(a, b, c, d);
    final int startOfB = HEADER + sizeOf(a);
    final int startOfC = startOfB + sizeOf(b);
    final int startOfD = startOfC + sizeOf(c);
    final int used = segment.getUsed();

    final EdgeBucketMask only200 = EdgeBucketMask.ofBucketIds(new int[] { 200 });
    final EdgeBucketMask only3 = EdgeBucketMask.ofBucketIds(new int[] { 3 });
    final EdgeBucketMask only4 = EdgeBucketMask.ofBucketIds(new int[] { 4 });
    final EdgeBucketMask none = EdgeBucketMask.ofBucketIds(new int[] { 9 });

    assertThat(segment.nextEntryInBuckets(HEADER, only3)).isEqualTo(HEADER);
    assertThat(segment.nextEntryInBuckets(HEADER, only200)).isEqualTo(startOfB);
    assertThat(segment.nextEntryInBuckets(startOfB, only200)).isEqualTo(startOfB);
    assertThat(segment.nextEntryInBuckets(startOfB, only3)).isEqualTo(startOfC);
    assertThat(segment.nextEntryInBuckets(HEADER, only4)).isEqualTo(startOfD);
    assertThat(segment.nextEntryInBuckets(startOfD, only4)).isEqualTo(startOfD);
    // NOTHING LEFT: THE END OF THE USED BYTES
    assertThat(segment.nextEntryInBuckets(HEADER, none)).isEqualTo(used);
    assertThat(segment.nextEntryInBuckets(startOfD, only3)).isEqualTo(used);
    assertThat(segment.nextEntryInBuckets(used, only3)).isEqualTo(used);
  }

  @Test
  void getRidAndSkipRidStepOverWholeEntries() {
    final long[] a = entry(3, 5, 4, 7);
    final long[] b = entry(200, 6, 300, 8);
    final MutableEdgeSegment segment = segmentHolding(a, b);

    final AtomicInteger position = new AtomicInteger(HEADER);
    assertThat(segment.getRID(position)).isEqualTo(rid(3, 5));
    assertThat(position.get()).isEqualTo(HEADER + Binary.getNumberSpace(3) + Binary.getNumberSpace(5));
    assertThat(segment.getRID(position)).isEqualTo(rid(4, 7));
    assertThat(position.get()).isEqualTo(HEADER + sizeOf(a));
    assertThat(segment.getRID(position)).isEqualTo(rid(200, 6));
    assertThat(segment.getRID(position)).isEqualTo(rid(300, 8));
    assertThat(position.get()).isEqualTo(segment.getUsed());

    final AtomicInteger skipping = new AtomicInteger(HEADER);
    segment.skipRID(skipping);
    assertThat(skipping.get()).isEqualTo(HEADER + Binary.getNumberSpace(3) + Binary.getNumberSpace(5));
    segment.skipRID(skipping);
    assertThat(skipping.get()).isEqualTo(HEADER + sizeOf(a));
    assertThat(segment.getRID(skipping)).isEqualTo(rid(200, 6));
  }

  @Test
  void toJSONListsTheEntriesNewestFirstAndTheIdentityOnlyOnRequest() {
    final MutableEdgeSegment segment = segmentHolding(entry(3, 5, 4, 7), entry(200, 6, 300, 8));

    final JSONObject json = segment.toJSON(false);
    assertThat(json.has("@rid")).isFalse();
    final JSONArray entries = json.getJSONArray("entries");
    assertThat(entries.length()).isEqualTo(2);
    assertThat(entries.getJSONObject(0).getString("edge")).isEqualTo("#3:5");
    assertThat(entries.getJSONObject(0).getString("vertex")).isEqualTo("#4:7");
    assertThat(entries.getJSONObject(1).getString("edge")).isEqualTo("#200:6");
    assertThat(entries.getJSONObject(1).getString("vertex")).isEqualTo("#300:8");

    // NO ENTRIES, NO ARRAY
    assertThat(segment(64).toJSON(true).has("entries")).isFalse();
  }

  @Test
  void thePreviousPointerSurvivesTheEntriesAndNeedsARID() {
    final MutableEdgeSegment segment = segment(64);

    assertThat(segment.getPreviousRID()).isNull();
    segment.setPrevious(rid(7, 1234));
    assertThat(segment.getPreviousRID()).isEqualTo(rid(7, 1234));

    assertThat(segment.addAtEndDirect(3, 5, 4, 7)).isTrue();
    assertThat(segment.add(rid(3, 6), rid(4, 8))).isTrue();
    assertThat(segment.getPreviousRID()).isEqualTo(rid(7, 1234));
    assertEntries(segment, entry(3, 6, 4, 8), entry(3, 5, 4, 7));

    assertThatThrownBy(() -> segment.setPrevious((RID) null)).isInstanceOf(IllegalArgumentException.class);
    assertThat(segment.getPreviousRID()).isEqualTo(rid(7, 1234));
  }

  /** A seeded sequence of operations against a plain list of entries: the segment must hold what the list holds after each one. */
  @Test
  void aSeededSequenceOfOperationsMatchesAListOfEntries() {
    final Random random = new Random(20260122L);
    final List<long[]> model = new ArrayList<>();
    final MutableEdgeSegment segment = segment(HEADER + 40 * 4);

    for (int step = 0; step < 4000; step++) {
      final long edgeBucket = 3 + random.nextInt(3);
      final long edgePosition = random.nextInt(4) == 0 ? -1 : random.nextInt(12);
      final long vertexBucket = 4 + random.nextInt(2);
      final long vertexPosition = random.nextInt(6);

      switch (random.nextInt(6)) {
      case 0 -> {
        final long[] e = entry(edgeBucket, edgePosition, vertexBucket, vertexPosition);
        final boolean fits = segment.getUsed() + sizeOf(e) <= HEADER + 40 * 4;
        assertThat(segment.add(rid(edgeBucket, edgePosition), rid(vertexBucket, vertexPosition))).isEqualTo(fits);
        if (fits)
          model.add(0, e);
      }
      case 1 -> {
        final long[] e = entry(edgeBucket, edgePosition, vertexBucket, vertexPosition);
        final boolean fits = segment.getUsed() + sizeOf(e) <= HEADER + 40 * 4;
        assertThat(segment.addAtEndDirect((int) edgeBucket, edgePosition, (int) vertexBucket, vertexPosition)).isEqualTo(fits);
        if (fits)
          model.add(e);
      }
      case 2 -> {
        int index = -1;
        for (int i = 0; i < model.size() && index < 0; i++)
          if (model.get(i)[0] == edgeBucket && model.get(i)[1] == edgePosition)
            index = i;
        assertThat(segment.removeEdge(rid(edgeBucket, edgePosition))).isEqualTo(index < 0 ? 0 : 1);
        if (index >= 0)
          model.remove(index);
      }
      case 3 -> {
        int index = -1;
        for (int i = 0; i < model.size() && index < 0; i++)
          if (model.get(i)[2] == vertexBucket && model.get(i)[3] == vertexPosition)
            index = i;
        assertThat(segment.removeVertex(rid(vertexBucket, vertexPosition))).isEqualTo(index < 0 ? 0 : 1);
        if (index >= 0)
          model.remove(index);
      }
      case 4 -> {
        int index = -1;
        int light = 0;
        for (int i = 0; i < model.size(); i++) {
          final long[] e = model.get(i);
          if (e[0] == edgeBucket && e[1] < 0 && e[2] == vertexBucket && e[3] == vertexPosition) {
            light++;
            if (index < 0)
              index = i;
          }
        }
        assertThat(segment.countLightEdges((int) edgeBucket, rid(vertexBucket, vertexPosition))).isEqualTo(light);
        assertThat(segment.containsLightEdge((int) edgeBucket, rid(vertexBucket, vertexPosition))).isEqualTo(light > 0);
        assertThat(segment.removeLightEdge((int) edgeBucket, rid(vertexBucket, vertexPosition))).isEqualTo(index < 0 ? 0 : 1);
        if (index >= 0)
          model.remove(index);
      }
      default -> {
        boolean contained = false;
        RID firstOnVertex = null;
        for (final long[] e : model) {
          contained |= e[0] == edgeBucket && e[1] == edgePosition;
          if (firstOnVertex == null && e[2] == vertexBucket && e[3] == vertexPosition)
            firstOnVertex = rid(e[0], e[1]);
        }
        assertThat(segment.containsEdge(rid(edgeBucket, edgePosition))).isEqualTo(contained);
        assertThat(segment.getFirstEdgeConnectedToVertex(rid(vertexBucket, vertexPosition), null)).isEqualTo(firstOnVertex);
      }
      }

      assertEntries(segment, model.toArray(new long[0][]));
      assertThat(segment.count(null)).isEqualTo(model.size());
    }
  }
}
