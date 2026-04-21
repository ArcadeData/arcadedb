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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RaftReplicatedDatabaseTest {

  @Test
  void implementsDatabaseInternal() {
    assertThat(DatabaseInternal.class.isAssignableFrom(RaftReplicatedDatabase.class)).isTrue();
  }

  @Test
  void parseResultSetFromJsonWithRecords() {
    final String json = "{\"result\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((String) results.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((int) results.get(0).getProperty("age")).isEqualTo(30);
    assertThat((String) results.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void parseResultSetFromJsonEmptyResult() {
    final String json = "{\"result\":[]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonNoResultKey() {
    final String json = "{\"user\":\"root\"}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonWithScalarValues() {
    final String json = "{\"result\":[42,\"hello\"]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((int) results.get(0).getProperty("value")).isEqualTo(42);
    assertThat((String) results.get(1).getProperty("value")).isEqualTo("hello");
  }
}
