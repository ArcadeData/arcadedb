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

/** The facets of a streamed result, in its {@code stats} trailer (final review B-M2), read without a server over a scripted body. */
class RemoteStreamingResultSetFacetsTest {
  private static final String ROW = "{\"record\":{\"n\":0}}\n";

  private static ResultSet stream(final String body) {
    return new RemoteStreamingResultSet(new BufferedReader(new StringReader(body)), json -> new ResultInternal(json.toMap()), false);
  }

  private static JSONObject stats(final int returned) {
    return new JSONObject().put("limit", -1).put("returned", returned).put("truncated", false);
  }

  private static String trailer(final JSONObject stats) {
    return new JSONObject().put("stats", stats) + "\n";
  }

  @Test
  void theFacetsOfTheTrailerAreReturnedOnceItIsRead() {
    final FacetResults sample = FacetResultsSamples.allKinds();
    try (final ResultSet rs = stream(ROW + ROW + trailer(stats(2).put("facets", sample.toJSON())))) {
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
  void aTrailerWithoutFacetsMeansNoFacets() {
    try (final ResultSet rs = stream(ROW + trailer(stats(1)))) {
      assertThat(rs.stream().count()).isEqualTo(1);
      assertThat(rs.getFacets()).isNull();
    }
  }

  @Test
  void malformedFacetsNameTheFacets() {
    try (final ResultSet rs = stream(ROW + trailer(stats(1).put("facets", 5)))) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(catchThrowable(rs::hasNext)).isInstanceOf(RemoteException.class)
          .hasMessage("Error while reading the facets of the streamed result");
    }
  }

  /** A line of its own named {@code facets} is no event of the encoding: refused like any unknown event, quoted as sent. */
  @Test
  void aFacetsLineIsAnUnknownEvent() {
    final String unknown = "{ \"facets\" : {} }";
    try (final ResultSet rs = stream(unknown + "\n")) {
      assertThat(catchThrowable(rs::hasNext)).isInstanceOf(RemoteException.class)
          .hasMessage("Unrecognized event in the streamed result: " + unknown);
    }
  }
}
