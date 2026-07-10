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

import com.arcadedb.serializer.json.JSONObject;

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

  @Test
  void copyAndRestoreRollBackIncrements() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.addPropertiesSet(2);
    final QueryStatistics snapshot = s.copy();
    // simulate a failed attempt that incremented further
    s.incNodesCreated();
    s.incRelationshipsCreated();
    s.addPropertiesSet(5);
    // roll back to the snapshot (as a retry would)
    s.restore(snapshot);
    assertThat(s.getNodesCreated()).isEqualTo(1);
    assertThat(s.getRelationshipsCreated()).isZero();
    assertThat(s.getPropertiesSet()).isEqualTo(2);
    // copy is independent: mutating the original must not change the snapshot
    s.incNodesCreated();
    assertThat(snapshot.getNodesCreated()).isEqualTo(1);
  }

  @Test
  void copiedContextSharesStatisticsAccumulator() {
    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.getStatistics().incNodesCreated();
    final CommandContext copy = ctx.copy();
    // The copy shares the same accumulator, so increments through it aggregate into one total
    // instead of vanishing.
    copy.getStatistics().incNodesCreated();
    assertThat(copy.getStatistics()).isSameAs(ctx.getStatistics());
    assertThat(ctx.getStatistics().getNodesCreated()).isEqualTo(2);
  }

  @Test
  void addMergesCountersFieldWise() {
    final QueryStatistics a = new QueryStatistics();
    a.incNodesCreated();
    a.addPropertiesSet(2);
    final QueryStatistics b = new QueryStatistics();
    b.incNodesCreated();
    b.incRelationshipsCreated();
    b.addPropertiesSet(3);
    a.add(b);
    assertThat(a.getNodesCreated()).isEqualTo(2);
    assertThat(a.getRelationshipsCreated()).isEqualTo(1);
    assertThat(a.getPropertiesSet()).isEqualTo(5);
    assertThat(a.containsUpdates()).isTrue();
  }

  @Test
  void addNullIsNoOp() {
    final QueryStatistics a = new QueryStatistics();
    a.incNodesCreated();
    a.add(null);
    assertThat(a.getNodesCreated()).isEqualTo(1);
  }

  @Test
  void toJSONEmitsAllCamelCaseCountersAndContainsUpdates() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.addPropertiesSet(2);
    final JSONObject json = s.toJSON();
    assertThat(json.getInt("nodesCreated")).isEqualTo(1);
    assertThat(json.getInt("propertiesSet")).isEqualTo(2);
    assertThat(json.getInt("relationshipsCreated")).isEqualTo(0);
    assertThat(json.getBoolean("containsUpdates")).isTrue();
  }
}
