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
package com.arcadedb.engine;

import com.arcadedb.database.BasicDatabase;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for MutablePage.move() mis-tracking the modified range for backward shifts.
 * When destPosition < startPosition the written bytes start at destPosition, but the buggy code
 * passed startPosition as the lower bound to updateModifiedRange, leaving [destPosition, startPosition)
 * untracked in the WAL delta.
 */
class MutablePageMoveTest {

  private static final int PAGE_SIZE = 1024;

  private MutablePage freshPage() {
    final BasicDatabase db = Mockito.mock(BasicDatabase.class);
    final PageId pageId = new PageId(db, 1, 0);
    // Use the 5-arg constructor so modifiedRangeFrom/To start at their sentinel values
    return new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, PAGE_SIZE);
  }

  @Test
  void backwardShiftModifiedRangeStartsAtDest() {
    final MutablePage page = freshPage();
    // dest(50) < start(100): backward shift — written bytes begin at destPosition
    page.move(100, 50, 60);
    final int[] range = page.getModifiedRange();
    assertThat(range[0]).isEqualTo(50 + BasePage.PAGE_HEADER_SIZE);
  }

  @Test
  void forwardShiftModifiedRangeStartsAtSource() {
    final MutablePage page = freshPage();
    // dest(100) > start(50): forward shift — written bytes start at destPosition but range from source
    page.move(50, 100, 60);
    final int[] range = page.getModifiedRange();
    assertThat(range[0]).isEqualTo(50 + BasePage.PAGE_HEADER_SIZE);
  }

  @Test
  void backwardShiftModifiedRangeCoversWrittenBytes() {
    final MutablePage page = freshPage();
    // dest=50, start=100, length=60 — written bytes: [50+HEADER, 50+HEADER+60-1]
    page.move(100, 50, 60);
    final int[] range = page.getModifiedRange();
    assertThat(range[0]).isLessThanOrEqualTo(50 + BasePage.PAGE_HEADER_SIZE);
    assertThat(range[1]).isGreaterThanOrEqualTo(50 + BasePage.PAGE_HEADER_SIZE + 60 - 1);
  }

  @Test
  void samePositionShiftTracksRange() {
    final MutablePage page = freshPage();
    page.move(50, 50, 60);
    final int[] range = page.getModifiedRange();
    assertThat(range[0]).isEqualTo(50 + BasePage.PAGE_HEADER_SIZE);
  }

  @Test
  void moveToEndOfPageDoesNotThrow() {
    final MutablePage page = freshPage();
    // dest=1006, length=10: last byte is at 1006+8+10-1 = 1023 = PAGE_SIZE-1 (inclusive upper bound)
    page.move(0, 1006, 10);
    final int[] range = page.getModifiedRange();
    assertThat(range[1]).isEqualTo(PAGE_SIZE - 1);
  }
}
