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

import com.arcadedb.query.facet.DistinctValue;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

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

  /**
   * Final review C-I8: the follower keeps the leader's row values as the JSON parse gives them, and the facet values the same way: a
   * date written as the row's text, a decimal and an integral number come back as the row's do, whatever type hint the facet carries.
   */
  @Test
  void facetValuesAreReadAsTheRowsAre() {
    final String body = "{\"result\":[{\"d\":\"2024-02-29\",\"p\":12.5,\"n\":7,\"s\":\"NaN\"}],\"facets\":{\"meta\":{\"exact\":true},"
        + "\"d\":{\"type\":\"terms\",\"source\":\"index\",\"@props\":\"value:" + Type.DATE.getId() + "\",\"buckets\":[{\"value\":\"2024-02-29\",\"count\":1}]},"
        + "\"p\":{\"type\":\"terms\",\"source\":\"scan\",\"buckets\":[{\"value\":12.5,\"count\":1}]},"
        + "\"n\":{\"type\":\"terms\",\"source\":\"index\",\"buckets\":[{\"value\":7,\"count\":1}]},"
        + "\"s\":{\"type\":\"terms\",\"source\":\"index\",\"buckets\":[{\"value\":\"NaN\",\"count\":1}]}}}";
    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(body);
    final Result row = rs.next();
    for (final String alias : List.of("d", "p", "n", "s")) {
      final Object facetValue = rs.getFacets().get(alias).buckets().getFirst().value();
      assertThat(facetValue).as(alias).isEqualTo(row.getProperty(alias));
      assertThat(facetValue.getClass()).as(alias).isEqualTo(row.getProperty(alias).getClass());
    }
  }

  /**
   * Plan 5 Task 14: the plan 5 kinds of a leader's answer, as the leader's HTTP serializer writes them: hierarchy attributes, a DISTINCT
   * value, a STATS facet whose sum and average are non-finite (written by name, read back as the numbers FacetStats holds) and a
   * date-time cursor kept as the row's text, as the follower keeps a row's value.
   */
  @Test
  void plan5KindsOfTheLeaderAnswer() {
    final String body = "{\"result\":[{\"p\":\"2024-02-29 10:30:15\"}],\"facets\":{\"meta\":{\"exact\":true,\"patched\":0,\"lagMs\":0},"
        + "\"t\":{\"type\":\"hierarchy\",\"source\":\"index\",\"buckets\":[{\"value\":\"t0/t1\",\"count\":4,\"parent\":\"t0\",\"depth\":1},"
        + "{\"value\":\"stray\",\"count\":1,\"parent\":null,\"depth\":0,\"orphan\":true}],\"otherCount\":0,\"missing\":2},"
        + "\"d\":{\"type\":\"distinct\",\"source\":\"index\",\"value\":42,\"exact\":true,\"missing\":1},"
        + "\"s\":{\"type\":\"stats\",\"source\":\"index\",\"count\":3,\"missing\":0,\"min\":1.5,\"max\":\"NaN\",\"sum\":\"NaN\",\"avg\":\"NaN\"},"
        + "\"p\":{\"type\":\"terms\",\"source\":\"index\",\"buckets\":[{\"value\":\"2024-02-29 10:30:15\",\"count\":1}],\"otherCount\":0,"
        + "\"missing\":0,\"after\":\"2024-02-29 10:30:15\"}}}";
    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(body);
    final Result row = rs.next();

    final FacetResult tree = rs.getFacets().get("t");
    assertThat(tree.buckets().get(0).attributes().getString("parent")).isEqualTo("t0");
    assertThat(tree.buckets().get(0).attributes().getInt("depth")).isEqualTo(1);
    assertThat(tree.buckets().get(1).attributes().getBoolean("orphan")).isTrue();
    assertThat(tree.missing()).isEqualTo(2L);
    assertThat(rs.getFacets().get("d").distinct()).isEqualTo(new DistinctValue(42L, true));
    final FacetStats stats = rs.getFacets().get("s").stats();
    assertThat(stats.count()).isEqualTo(3L);
    assertThat(stats.sum()).isEqualTo(Double.NaN);
    assertThat(stats.avg()).isEqualTo(Double.NaN);
    // the extremes are values of the property: kept as the JSON parse gives them, like a row's
    assertThat(stats.max()).isEqualTo("NaN");
    final FacetResult paged = rs.getFacets().get("p");
    assertThat(paged.hasAfter()).isTrue();
    assertThat(paged.after()).isEqualTo(row.getProperty("p"));
  }

  @Test
  void noFacetsMemberMeansNoFacets() {
    assertThat(RaftReplicatedDatabase.parseResultSetFromJson("{\"result\":[]}").getFacets()).isNull();
  }
}
