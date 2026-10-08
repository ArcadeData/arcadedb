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
package com.arcadedb.exception;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9473: rebuilding a {@link DuplicatedKeyException} from the pipe-separated {@code exceptionArgs} the server
 * sends ({@code indexName|keys|rid}) must never throw, whatever the server put there.
 */
class DuplicatedKeyExceptionTest {

  @Test
  void wellFormedArgsRebuildTheTypedException() {
    final DuplicatedKeyException dup = DuplicatedKeyException.fromExceptionArgs("Account[id]|[42]|#7:1");

    assertThat(dup).isNotNull();
    assertThat(dup.getIndexName()).isEqualTo("Account[id]");
    assertThat(dup.getKeys()).isEqualTo("[42]");
    assertThat(dup.getCurrentIndexedRID()).isEqualTo(new RID(7, 1L));
  }

  /**
   * A key VALUE is customer data and can contain the separator itself. The index name is the first segment and the RID
   * the last, so whatever lies between belongs to the keys.
   */
  @Test
  void aKeyContainingThePipeSeparatorStaysInTheKeys() {
    final DuplicatedKeyException dup = DuplicatedKeyException.fromExceptionArgs("Account[name]|[a|b||c]|#7:1");

    assertThat(dup).isNotNull();
    assertThat(dup.getIndexName()).isEqualTo("Account[name]");
    assertThat(dup.getKeys()).isEqualTo("[a|b||c]");
    assertThat(dup.getCurrentIndexedRID()).isEqualTo(new RID(7, 1L));
  }

  /** The server serializes a null current RID as the string "null": still a well-formed answer. */
  @Test
  void aNullRidTokenRebuildsWithANullRid() {
    final DuplicatedKeyException dup = DuplicatedKeyException.fromExceptionArgs("Account[id]|[42]|null");

    assertThat(dup).isNotNull();
    assertThat(dup.getKeys()).isEqualTo("[42]");
    assertThat(dup.getCurrentIndexedRID()).isNull();
  }

  @Test
  void aConcealedKeysSegmentIsKeptAsIs() {
    final DuplicatedKeyException dup = DuplicatedKeyException.fromExceptionArgs("Account[id]|[concealed]|#7:1");

    assertThat(dup).isNotNull();
    assertThat(dup.getKeys()).isEqualTo("[concealed]");
  }

  @ParameterizedTest
  @ValueSource(strings = { "", "only-one-part", "index|keys", "index|keys|not-a-rid", "index|keys|#7", "index|keys|#a:b",
      "index|keys|", "|" })
  void malformedArgsYieldNullInsteadOfThrowing(final String exceptionArgs) {
    assertThat(DuplicatedKeyException.fromExceptionArgs(exceptionArgs)).isNull();
  }

  @Test
  void nullArgsYieldNull() {
    assertThat(DuplicatedKeyException.fromExceptionArgs(null)).isNull();
  }
}
