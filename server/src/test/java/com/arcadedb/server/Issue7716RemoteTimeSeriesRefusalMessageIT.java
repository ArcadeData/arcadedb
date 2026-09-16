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
package com.arcadedb.server;

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQuery.Aggregation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7716: {@link RemoteDatabase} hid the server's time-series refusal in the exception CAUSE, while
 * {@code RemoteGrpcDatabase} put it in the message.
 * <p>
 * The server names the offending member in its refusal - which is the whole point of issues #7334, #7340 and
 * #7675 - and the HTTP client used to answer a fixed {@code "Error on time series query"} with the server's
 * {@code error} body hung off it as the cause. Nothing was lost, but what a caller SEES first said nothing it
 * could act on: an application logging {@code e.getMessage()}, which is the common thing to do, logged
 * {@code Error on time series query} for a typo the server had been careful enough to name, and the same
 * assertion could not be written against both protocols -
 * {@code Issue7675GrpcTimeSeriesProjectionIT} had to use {@code hasStackTraceContaining} for exactly this.
 * <p>
 * Every case below is asserted with {@code hasMessageContaining}, which is the assertion that used to fail.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7716RemoteTimeSeriesRefusalMessageIT extends BaseGraphServerTest {

  private static final String TYPE = "refusalmetric";

  @Override
  protected int getServerCount() {
    return 1;
  }

  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  private void createType(final RemoteDatabase database) {
    if (!database.query("sql", "SELECT FROM schema:types WHERE name = '" + TYPE + "'").hasNext())
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
          + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
  }

  /**
   * The case #7716 was reported for: a projection naming a column the type does not have. The server answers
   * 400 with that sentence in {@code error} since issue #7675, and the caller has to be able to read it off
   * {@code getMessage()} - which is the assertion {@code Issue7675GrpcTimeSeriesProjectionIT} had to write as
   * {@code hasStackTraceContaining} instead, and said so.
   */
  @Test
  void anUnresolvableProjectionIsNamedInTheMessage() {
    try (final RemoteDatabase database = remote()) {
      createType(database);

      assertThatThrownBy(() -> database.timeSeriesQuery(new TimeSeriesQuery(TYPE).fields("nosuchcolumn")))
          .hasMessageContaining("nosuchcolumn");
    }
  }

  /** A tag name that resolves to no TAG column, refused by name since issue #7334. */
  @Test
  void anUnresolvableTagIsNamedInTheMessage() {
    try (final RemoteDatabase database = remote()) {
      createType(database);

      assertThatThrownBy(() -> database.timeSeriesQuery(new TimeSeriesQuery(TYPE).tag("nosuchtag", "x")))
          .hasMessageContaining("nosuchtag");
    }
  }

  /** And an aggregation over a field the type does not carry. */
  @Test
  void anUnknownAggregationFieldIsNamedInTheMessage() {
    try (final RemoteDatabase database = remote()) {
      createType(database);

      assertThatThrownBy(() -> database.timeSeriesQuery(
          new TimeSeriesQuery(TYPE).aggregate(1_000L, new Aggregation("nosuchfield", AggregationType.SUM))))
          .hasMessageContaining("nosuchfield");
    }
  }

  /**
   * Issue #7725 over the wire: aggregating a column no storage layer can read as a number is now refused by
   * name, where it used to answer zeros until compaction ran and an internal error afterwards.
   */
  @Test
  void aggregatingATagIsRefusedByNameOverTheWire() {
    try (final RemoteDatabase database = remote()) {
      createType(database);

      assertThatThrownBy(() -> database.timeSeriesQuery(
          new TimeSeriesQuery(TYPE).aggregate(1_000L, new Aggregation("location", AggregationType.SUM))))
          .hasMessageContaining("location")
          .hasMessageContaining("is not stored as a number");
    }
  }

  /** The {@code /ts/latest} endpoint takes the same path through the client, so it gets the same treatment. */
  @Test
  void theLatestEndpointCarriesItsReasonInTheMessageToo() {
    try (final RemoteDatabase database = remote()) {
      createType(database);
      database.timeSeriesWrite(List.of(
          new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 21.0))));

      assertThatThrownBy(() -> database.timeSeriesLatest(TYPE, "nosuchtag", "x"))
          .hasMessageContaining("nosuchtag");
    }
  }
}
