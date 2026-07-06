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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSetStatisticsTest {
  @Test
  void defaultResultSetHasNoStatistics() {
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    assertThat(rs.getStatistics()).isEmpty();
  }

  @Test
  void attachedStatisticsAreReturned() {
    final QueryStatistics stats = new QueryStatistics();
    stats.incNodesCreated();
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    rs.setStatistics(stats);
    assertThat(rs.getStatistics()).isPresent();
    assertThat(rs.getStatistics().get().getNodesCreated()).isEqualTo(1);
  }
}
