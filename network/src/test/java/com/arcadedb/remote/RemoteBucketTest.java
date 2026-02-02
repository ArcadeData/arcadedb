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
package com.arcadedb.remote;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteBucketTest {

  private RemoteBucket bucket;

  @BeforeEach
  void setUp() {
    bucket = new RemoteBucket("testBucket");
  }

  @Test
  void getName() {
    assertThat(bucket.getName()).isEqualTo("testBucket");
  }

  @Test
  void createRecordThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.createRecord(null, false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void updateRecordThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.updateRecord(null, false))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getRecordThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.getRecord(new RID(null, 0, 0)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void existsRecordThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.existsRecord(new RID(null, 0, 0)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void deleteRecordThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.deleteRecord(new RID(null, 0, 0)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void scanThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.scan(null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void iteratorThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.iterator())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void inverseIteratorThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.inverseIterator())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void countThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.count())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getFileIdThrowsUnsupportedOperationException() {
    assertThatThrownBy(() -> bucket.getFileId())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
