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

import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #11470: a client timeout on a command the server went on to commit is not a lost user. */
class DatabaseWrapperClientTimeoutTest {

  @Test
  void aTimeoutAnywhereInTheCauseChainIsAClientTimeout() {
    assertThat(DatabaseWrapper.isClientTimeout(new RuntimeException("wrapped", new SocketTimeoutException("Read timed out")))).isTrue();
    assertThat(DatabaseWrapper.isClientTimeout(new RuntimeException(new IllegalStateException(new HttpTimeoutException("t"))))).isTrue();
  }

  @Test
  void anyOtherFailureIsNot() {
    assertThat(DatabaseWrapper.isClientTimeout(new RuntimeException("Duplicated key"))).isFalse();
    assertThat(DatabaseWrapper.isClientTimeout(null)).isFalse();
  }
}
