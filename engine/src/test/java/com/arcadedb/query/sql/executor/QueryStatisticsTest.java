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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryStatisticsTest {
  @Test
  void freshInstanceHasNoUpdates() {
    final QueryStatistics s = new QueryStatistics();
    assertThat(s.containsUpdates()).isFalse();
    assertThat(s.getNodesCreated()).isZero();
  }

  @Test
  void incrementsAreTracked() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.incNodesCreated();
    s.incRelationshipsCreated();
    s.addPropertiesSet(3);
    s.addLabelsAdded(2);
    assertThat(s.getNodesCreated()).isEqualTo(2);
    assertThat(s.getRelationshipsCreated()).isEqualTo(1);
    assertThat(s.getPropertiesSet()).isEqualTo(3);
    assertThat(s.getLabelsAdded()).isEqualTo(2);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void addingZeroPropertiesKeepsNoUpdatesWhenNothingElseChanged() {
    final QueryStatistics s = new QueryStatistics();
    s.addPropertiesSet(0);
    s.addLabelsAdded(0);
    assertThat(s.containsUpdates()).isFalse();
  }
}
