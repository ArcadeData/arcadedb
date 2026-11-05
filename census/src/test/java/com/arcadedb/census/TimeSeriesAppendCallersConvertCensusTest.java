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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10107: three production paths append a batch to a time series engine - the line protocol and gRPC
 * ({@code TimeSeriesGateway.write}), the Prometheus remote write and the JSONL importer - and the third handed the engine the raw
 * values of its source, so a typed tag written as text aborted the whole import. The conversion had been added to the other two
 * call sites one by one (issues #9809, #9972) and then moved into {@code TimeSeriesStorableRows} (issue #10030), which is now the
 * ONE way a client-facing path reaches the storage: {@code TimeSeriesStorableRows.convert} builds the grid,
 * {@code TimeSeriesStorableRows.appendTo} appends it. Issue #10381 found a fourth, the embedded async API, which reached the same
 * storage through {@code newRowSource} and {@code shard.appendSamples}.
 * <p>
 * The census ranges over the authority (#11288): every call of an entry point into the storage (any {@code append...}
 * method, and {@code newRowSource}) on the storage classes - {@code TimeSeriesEngine}, {@code TimeSeriesShard},
 * {@code TimeSeriesBucket} and anything that extends them - or method reference to one, in the compiled classes of every module
 * ({@link CompiledClasses}). The source-text census it replaces matched the three names after a dot on any receiver, line by line,
 * so a call split after the dot passed it, and so did {@code appendSamplesWithoutRefresh}, the call
 * {@code SaveElementStep} actually makes, which the census allowed by file without ever matching; and it would have flagged a
 * caller of the public async {@code appendSamples}, which converts on the calling thread. A new endpoint calling the storage
 * with a grid of its own fails here and is sent to the converting entry points. Each path is exercised with a text value for a
 * typed column by its own test: {@code Issue9809LineProtocolTemporalValueTest},
 * {@code Issue9972PrometheusTypedTagIT}, {@code Issue10107JsonlTimeSeriesUnstorableValueTest} and
 * {@code Issue10381AsyncAppendSamplesConstraintsTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesAppendCallersConvertCensusTest {
  private static final String      PACKAGE     = "com/arcadedb/engine/timeseries/";
  private static final Set<String> STORAGE     = Set.of(PACKAGE + "TimeSeriesEngine", PACKAGE + "TimeSeriesShard",
      PACKAGE + "TimeSeriesBucket");
  /** Every {@code append...} of the storage writes samples, and {@code newRowSource} builds the source one writes. */
  private static final String      APPEND      = "append";
  private static final String      ROW_SOURCE  = "newRowSource";

  private static final String STORAGE_ITSELF = "the storage itself: an overload delegating to the one that writes";
  private static final String CONVERTED      = "appends the grid TimeSeriesStorableRows converted and checked";

  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("engine/timeseries/TimeSeriesEngine#appendSamples", STORAGE_ITSELF),
      Map.entry("engine/timeseries/TimeSeriesEngine#appendBatch", STORAGE_ITSELF),
      Map.entry("engine/timeseries/TimeSeriesEngine#appendBatchToShards", "the storage itself: spreads a converted batch over the shards"),
      Map.entry("engine/timeseries/TimeSeriesEngine#appendSamplesWithoutRefresh", STORAGE_ITSELF),
      Map.entry("engine/timeseries/TimeSeriesEngine#appendToOneShard", "the storage itself: hands a converted batch to its shard"),
      Map.entry("engine/timeseries/TimeSeriesShard#appendSamples", STORAGE_ITSELF),
      Map.entry("engine/timeseries/TimeSeriesBucket#appendSamples", STORAGE_ITSELF),
      Map.entry("engine/timeseries/TimeSeriesStorableRows#appendTo", CONVERTED),
      Map.entry("engine/timeseries/TimeSeriesStorableRows#requireStorable", "builds the source it then checks against the columns"),
      Map.entry("query/sql/executor/SaveElementStep#saveToTimeSeries",
          "converts and checks the one row of the statement itself before appending it"),
      Map.entry("database/async/DatabaseAsyncAppendSamples#execute",
          "appends a source the executor converted and checked on the calling thread before scheduling it "
              + "(TimeSeriesStorableRows.requireStorable)")));

  @Test
  void noProductionPathAppendsAGridOfItsOwn() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Map<String, Set<String>> callSites = callSitesOf(classes);
    assertThat(callSites).as("the census must reach the converting entry point it is known to cover")
        .containsKey("engine/timeseries/TimeSeriesStorableRows#appendTo");

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
        "production classes that reach the storage of a time series engine (append..., newRowSource) with a "
            + "grid of their own, bypassing the conversion and the column constraints every ingestion path shares: build the rows "
            + "with TimeSeriesStorableRows (convert + appendTo, or requireStorable for the async API) (issues #10107, #10381)")
        .isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as("REVIEWED entries that no longer reach the storage: remove them")
        .isEmpty();
  }

  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    final Set<String> owners = new HashSet<>();
    for (final String storage : STORAGE)
      owners.addAll(CompiledClasses.implementationsOf(storage, classes));
    return CompiledClasses.callSitesOf(classes, (owner, name, desc) -> owners.contains(owner) && (name.startsWith(APPEND) || ROW_SOURCE.equals(name)));
  }
}
