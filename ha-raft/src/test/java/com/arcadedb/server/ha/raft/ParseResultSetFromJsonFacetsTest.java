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
package com.arcadedb.server.ha.raft;

import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The leader's answer to a forwarded command keeps its {@code facets} member when the follower parses it back into a result set. */
class ParseResultSetFromJsonFacetsTest {

  @Test
  void facetsSurviveTheLeaderAnswer() {
    final String sample = FacetResultsSamples.allKinds().toJSON().toString();
    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson("{\"result\":[{\"n\":1}],\"facets\":" + sample + "}");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.getFacets().toJSON().toString()).isEqualTo(sample);
  }

  @Test
  void plan5MembersSurviveTheLeaderAnswer() {
    final String sample = FacetResultsSamples.plan5Members().toJSON().toString();
    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson("{\"result\":[],\"facets\":" + sample + "}");
    assertThat(new JSONObject(rs.getFacets().toJSON().toString()).toString()).isEqualTo(sample);
  }

  @Test
  void noFacetsMemberMeansNoFacets() {
    assertThat(RaftReplicatedDatabase.parseResultSetFromJson("{\"result\":[]}").getFacets()).isNull();
  }
}
