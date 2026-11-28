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
package com.arcadedb.bench.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CpusetsTest {
  @Test
  void splitsTwelveIntoThreeFours() {
    assertThat(Cpusets.split("0-11", 3)).containsExactly("0-3", "4-7", "8-11");
  }

  @Test
  void keepsListOrderAndRanges() {
    assertThat(Cpusets.split("0-3,8-11", 2)).containsExactly("0-3", "8-11");
  }

  @Test
  void splitsAcrossRangeBoundaries() {
    assertThat(Cpusets.split("0-3,8,10-11", 2)).containsExactly("0-3", "8,10-11");
  }

  @Test
  void unevenSplitGivesTheExtraToTheFirst() {
    assertThat(Cpusets.split("0-6", 3)).containsExactly("0-2", "3-4", "5-6");
  }

  @Test
  void fewerCpusThanNodesShareTheWholeSet() {
    assertThat(Cpusets.split("0-1", 3)).containsExactly("0-1", "0-1", "0-1");
  }

  @Test
  void singleCpuGroupIsNotARange() {
    assertThat(Cpusets.split("0-3", 4)).containsExactly("0", "1", "2", "3");
  }

  @Test
  void emptyMeansUnpinned() {
    assertThat(Cpusets.split("", 3)).containsExactly("", "", "");
  }

  @Test
  void nodeSizing() {
    assertThat(Cpusets.nodeMemGb(16, 3)).isEqualTo(5);
    assertThat(Cpusets.nodeHeap(5)).isEqualTo("3g");
    assertThat(Cpusets.nodeMemGb(6, 3)).isEqualTo(2);
    assertThat(Cpusets.nodeHeap(2)).isEqualTo("1g");
    assertThat(Cpusets.nodeMemGb(2, 3)).isEqualTo(2);
  }
}
