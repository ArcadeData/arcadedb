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
package com.arcadedb.remote;

import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** The {@code facets} line of a streamed result, read without a server over a scripted body. */
class RemoteStreamingResultSetFacetsTest {
  private static final String ROW = "{\"record\":{\"n\":0}}\n";

  private static ResultSet stream(final String body) {
    return new RemoteStreamingResultSet(new BufferedReader(new StringReader(body)), json -> new ResultInternal(json.toMap()), false);
  }

  private static String stats(final int returned) {
    return new JSONObject().put("stats", new JSONObject().put("limit", -1).put("returned", returned).put("truncated", false)) + "\n";
  }

  @Test
  void facetsLineIsKeptAndReturnedAfterTheTrailer() {
    final FacetResults sample = FacetResultsSamples.allKinds();
    final String facetsLine = new JSONObject().put("facets", sample.toJSON()) + "\n";
    try (final ResultSet rs = stream(ROW + ROW + facetsLine + stats(2))) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.getFacets()).isNull();
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
      assertThat(rs.getFacets().toJSON().toString()).isEqualTo(sample.toJSON().toString());
    }
  }

  @Test
  void noFacetsLineMeansNoFacets() {
    try (final ResultSet rs = stream(ROW + stats(1))) {
      assertThat(rs.stream().count()).isEqualTo(1);
      assertThat(rs.getFacets()).isNull();
    }
  }

  /** A stream cut after its facets line is still incomplete: the facets are not a trailer. */
  @Test
  void facetsWithoutStatsIsIncomplete() {
    final String facetsLine = new JSONObject().put("facets", FacetResultsSamples.allKinds().toJSON()) + "\n";
    try (final ResultSet rs = stream(ROW + facetsLine)) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(catchThrowable(rs::hasNext)).isInstanceOf(RemoteException.class).hasMessageContaining("'stats' trailer");
    }
  }

  @Test
  void aMalformedFacetsLineNamesTheFacets() {
    try (final ResultSet rs = stream(ROW + "{\"facets\":5}\n" + stats(1))) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(catchThrowable(rs::hasNext)).isInstanceOf(RemoteException.class)
          .hasMessage("Error while reading the facets of the streamed result");
    }
  }

  /** An unknown event is quoted as the server sent it, byte for byte, as before the facets line existed. */
  @Test
  void anUnknownEventIsQuotedAsSent() {
    final String unknown = "{ \"other\" : 1 }";
    try (final ResultSet rs = stream(unknown + "\n")) {
      assertThat(catchThrowable(rs::hasNext)).isInstanceOf(RemoteException.class)
          .hasMessage("Unrecognized event in the streamed result: " + unknown);
    }
  }

  /** Many facets lines (a hostile peer) are read in a loop, not one stack frame each. */
  @Test
  void manyFacetsLinesDoNotRecurse() {
    final String facetsLine = new JSONObject().put("facets", new JSONObject()) + "\n";
    final String body = ROW + facetsLine.repeat(50_000) + stats(1);
    try (final ResultSet rs = stream(body)) {
      assertThat(rs.stream().count()).isEqualTo(1);
      assertThat(rs.getFacets()).isNotNull();
    }
  }
}
