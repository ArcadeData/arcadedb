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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the replay-coverage bookkeeping introduced by issue #5596: a commit-time page merge may only
 * re-derive a page whose every modified byte was written inside a declaration naming that merge. Anything written
 * outside such a declaration permanently disqualifies the page - which is what turns "every writer must remember to
 * poison" into "only a writer that says so is replayed".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MutablePageWriteCoverageTest {
  private static final int PAGE_SIZE = 4096;

  private MutablePage newPage() {
    // NOT the (pageId,size) constructor: that one marks the whole page modified (a brand-new page), which would
    // start every test from an already-uncovered state.
    // A null PageId keeps this a pure unit test (a real one needs a database): the coverage bookkeeping is per-page
    // state that never dereferences the page id. If that ever changes, give these pages a real PageId.
    return new MutablePage(null, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }

  @Test
  void anUntouchedPageIsCoveredByEveryMechanism() {
    final MutablePage page = newPage();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isTrue();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isTrue();
  }

  @Test
  void anUndeclaredWriteDisqualifiesEveryMechanism() {
    final MutablePage page = newPage();
    page.writeInt(0, 42);

    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isFalse();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();
  }

  @Test
  void aDeclaredWriteCoversOnlyTheMechanismsItNames() {
    final MutablePage page = newPage();

    final int previous = page.beginCoveredWrite(MutablePage.COVERAGE_SLOT_MERGE);
    try {
      page.writeInt(0, 42);
    } finally {
      page.endCoveredWrite(previous);
    }

    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isTrue();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isFalse();
  }

  @Test
  void aWriteDeclaredForAllMechanismsCoversAllOfThem() {
    final MutablePage page = newPage();

    final int previous = page.beginCoveredWrite(MutablePage.COVERAGE_ALL_MERGES);
    try {
      page.writeByteArray(10, new byte[] { 1, 2, 3 });
      page.move(10, 20, 3);
    } finally {
      page.endCoveredWrite(previous);
    }

    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isTrue();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isTrue();
  }

  /**
   * The whole point of the check: one forgotten byte after any number of correctly declared writes is enough to
   * refuse the merge. Coverage is sticky, never re-earned.
   */
  @Test
  void oneUndeclaredByteAfterDeclaredWritesStillDisqualifies() {
    final MutablePage page = newPage();

    final int previous = page.beginCoveredWrite(MutablePage.COVERAGE_SLOT_MERGE);
    try {
      page.writeByteArray(100, new byte[64]);
    } finally {
      page.endCoveredWrite(previous);
    }
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isTrue();

    // An untracked writer touching ONE byte - the #5596 bug class.
    page.writeByte(500, (byte) 7);

    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();

    // ...and it cannot be undone by declaring more writes afterwards.
    final int previous2 = page.beginCoveredWrite(MutablePage.COVERAGE_SLOT_MERGE);
    try {
      page.writeByteArray(200, new byte[8]);
    } finally {
      page.endCoveredWrite(previous2);
    }
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();
  }

  /**
   * A nested declaration must not leak: after the inner writer restores what it replaced, the outer declaration is
   * back in force, and after the outer one ends writes are undeclared again. The nesting production actually uses is
   * an inner scope that declares NOTHING - a writer disclaiming coverage for one branch, as
   * {@code createRecordInternal} does for a multi-page record - inside an outer declared one.
   */
  @Test
  void nestedDeclarationsRestoreTheOuterScope() {
    final MutablePage page = newPage();

    final int outer = page.beginCoveredWrite(MutablePage.COVERAGE_ALL_MERGES);
    try {
      final int inner = page.beginCoveredWrite(0);
      try {
        page.writeByte(10, (byte) 1);   // undeclared: disqualifies both mechanisms
      } finally {
        page.endCoveredWrite(inner);
      }
      // Back to the outer (all-mechanism) declaration: this write does NOT disqualify anything further.
      page.writeByte(11, (byte) 1);
    } finally {
      page.endCoveredWrite(outer);
    }

    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isFalse();

    // ...and the outermost scope really was restored to "no declaration": on a clean page a declared write is
    // covered, and only what comes AFTER the end is not.
    final MutablePage clean = newPage();
    final int scope = clean.beginCoveredWrite(MutablePage.COVERAGE_SLOT_MERGE);
    try {
      clean.writeByte(10, (byte) 1);
    } finally {
      clean.endCoveredWrite(scope);
    }
    assertThat(clean.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isTrue();
    clean.writeByte(12, (byte) 1);
    assertThat(clean.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();
  }

  /**
   * The guard on the one way this backstop could become the hazard it removes: a writer that opens a declaration and
   * never restores it would silently vouch for every later write to that page. Opening a second declaration while one
   * is still live is exactly that footprint, and trips an assertion (enabled under {@code -ea}, as surefire runs).
   */
  @Test
  void aLeakedDeclarationIsCaughtByAnAssertion() {
    // Guard the guard: with assertions disabled this test would pass vacuously, so fail loudly instead.
    assertThat(MutablePageWriteCoverageTest.class.desiredAssertionStatus())
        .as("this test needs -ea; surefire enables assertions by default").isTrue();

    final MutablePage page = newPage();
    page.beginCoveredWrite(MutablePage.COVERAGE_SLOT_MERGE);   // deliberately never restored
    page.writeByte(10, (byte) 1);

    assertThatThrownBy(() -> page.beginCoveredWrite(MutablePage.COVERAGE_EDGE_APPEND_MERGE))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("leaked");
  }

  @Test
  void aBrandNewPageIsNeverCovered() {
    // The (pageId,size) constructor marks the whole page modified: a page created by the transaction has no
    // committed version to rebase against, and must never look re-derivable.
    final MutablePage page = new MutablePage(null, PAGE_SIZE);
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE)).isFalse();
    assertThat(page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE)).isFalse();
  }
}
