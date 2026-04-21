/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReadConsistencyContextTest {

  @Test
  void setAndClearContext() {
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, 42);
    final var ctx = RaftReplicatedDatabase.getReadConsistencyContext();
    assertThat(ctx).isNotNull();
    assertThat(ctx.consistency()).isEqualTo(Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    assertThat(ctx.readAfterIndex()).isEqualTo(42);

    RaftReplicatedDatabase.removeReadConsistencyContext();
    assertThat(RaftReplicatedDatabase.getReadConsistencyContext()).isNull();
  }

  @Test
  void contextIsNullByDefault() {
    assertThat(RaftReplicatedDatabase.getReadConsistencyContext()).isNull();
  }
}
