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
package com.arcadedb.bench.store;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultsStoreTest {
  @Test
  void anHttpsUrlKeepsItsScheme() {
    assertThat(ResultsStore.serverAddress(URI.create("https://perf-results:2480"))).isEqualTo("https://perf-results");
  }

  @Test
  void anHttpUrlKeepsItsScheme() {
    assertThat(ResultsStore.serverAddress(URI.create("http://perf-results:2480"))).isEqualTo("http://perf-results");
  }

  @Test
  void anotherSchemeIsRefused() {
    assertThatThrownBy(() -> ResultsStore.serverAddress(URI.create("ftp://perf-results:2480")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("http");
  }
}
