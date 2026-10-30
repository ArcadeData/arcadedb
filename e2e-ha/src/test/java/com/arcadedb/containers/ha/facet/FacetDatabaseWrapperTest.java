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

package com.arcadedb.containers.ha.facet;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.test.support.FacetDatabaseWrapper;
import com.arcadedb.test.support.FacetDatabaseWrapper.OpCounts;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure unit tests of {@link FacetDatabaseWrapper}'s rules, kept in the unit lane (the load-tests module runs only nightly):
 * <ul>
 *   <li>a duplicate key counts as an acknowledged write only for an INSERT that was retried, since only then can its own earlier
 *   attempt have committed behind a retryable error;</li>
 *   <li>a writer thread's outcome proves that it wrote, which is the premise of every facets-under-writes assertion.</li>
 * </ul>
 */
class FacetDatabaseWrapperTest {
  private static final DuplicatedKeyException DUPLICATE = new DuplicatedKeyException("FacetAsset[aid]", "[42]", null);

  @Test
  void duplicateKeyOnARetriedInsertIsAcked() {
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(true, 1, DUPLICATE)).isTrue();
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(true, FacetDatabaseWrapper.RETRIES, DUPLICATE)).isTrue();
  }

  @Test
  void duplicateKeyOnTheFirstAttemptIsAFailure() {
    // no attempt of this wrapper can have committed before; an HTTP client-internal retry can, which the Javadoc of the rule names
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(true, 0, DUPLICATE)).isFalse();
  }

  @Test
  void duplicateKeyOnAnUpdateIsAFailure() {
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(false, 1, DUPLICATE)).isFalse();
  }

  @Test
  void anotherErrorOnARetriedInsertIsAFailure() {
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(true, 1, new ConcurrentModificationException("conflict"))).isFalse();
    assertThat(FacetDatabaseWrapper.duplicateMeansAcked(true, 1, new IllegalStateException("broken"))).isFalse();
  }

  @Test
  void aWriterThatWroteWithoutFailuresPasses() {
    final OpCounts counts = new OpCounts(10, 0, 2);
    assertThat(FacetDatabaseWrapper.assertWrote(counts, "writer", true)).isSameAs(counts);
  }

  @Test
  void aWriterUnderAFaultMayFailSomeWrites() {
    assertThat(FacetDatabaseWrapper.assertWrote(new OpCounts(10, 3, 0), "writer", false).failed()).isEqualTo(3L);
    assertThatThrownBy(() -> FacetDatabaseWrapper.assertWrote(new OpCounts(10, 3, 0), "writer", true))
        .isInstanceOf(AssertionError.class).hasMessageContaining("writer");
  }

  @Test
  void aWriterThatAckedNothingFails() {
    // every write failed (a broken statement, a refusing leader, an auth lockout): the nodes then agree trivially
    assertThatThrownBy(() -> FacetDatabaseWrapper.assertWrote(new OpCounts(0, 1_000, 0), "writer", false))
        .isInstanceOf(AssertionError.class).hasMessageContaining("acked");
  }

  @Test
  void aWriterThatThrewOrNeverReportedFails() {
    assertThatThrownBy(() -> FacetDatabaseWrapper.assertWrote(new IllegalStateException("boom"), "writer", false))
        .isInstanceOf(AssertionError.class).hasMessageContaining("boom");
    assertThatThrownBy(() -> FacetDatabaseWrapper.assertWrote(null, "writer", false)).isInstanceOf(AssertionError.class);
  }
}
