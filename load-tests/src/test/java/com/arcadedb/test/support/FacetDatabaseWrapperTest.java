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
package com.arcadedb.test.support;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retry rule of {@link FacetDatabaseWrapper}: a duplicate key counts as an acknowledged write only for an INSERT that was retried,
 * since only then can its own earlier attempt have committed behind a retryable error.
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
    // nothing of this writer can have committed before: the key belongs to someone else, a real defect of the key scheme
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
}
