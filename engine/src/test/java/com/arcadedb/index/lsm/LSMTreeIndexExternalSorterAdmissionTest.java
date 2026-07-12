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
package com.arcadedb.index.lsm;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeIndexExternalSorterAdmissionTest {
  @Test
  void capsFanInByMemoryBudget() throws Exception {
    assertThat(LSMTreeIndexExternalSorter.selectMergeFanIn(128, 1L << 20, 1_000)).isEqualTo(2);
  }

  @Test
  void retainsOperatorCapWhenResourcesAllowIt() throws Exception {
    assertThat(LSMTreeIndexExternalSorter.selectMergeFanIn(8, 1L << 30, 1_000)).isEqualTo(8);
  }

  @Test
  void rejectsInsufficientFileDescriptors() {
    assertThatThrownBy(() -> LSMTreeIndexExternalSorter.selectMergeFanIn(8, 1L << 30, 2))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Insufficient file descriptors");
  }
}
