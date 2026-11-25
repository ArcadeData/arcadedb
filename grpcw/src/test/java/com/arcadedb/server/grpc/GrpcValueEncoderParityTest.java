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
package com.arcadedb.server.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11207: {@link GrpcTypeConverter#toGrpcValue(Object)}, the encoder of the search and time series RPCs, matched
 * {@code List} but no other container, so a {@link JSONArray} - which is an {@code Iterable} and not a {@code List}, and is
 * what a search hit's array property is - reached the client as its JSON text, where the query path sends the same property
 * as a list. The two encoders now share their scalar and container arms, so a value of a given Java type is encoded the same
 * way whichever RPC returned it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcValueEncoderParityTest {
  private static final String DB_PATH = "target/databases/grpc-value-encoder-parity-test";

  @Test
  void aJsonArrayIsAList() {
    final GrpcValue value = GrpcTypeConverter.toGrpcValue(new JSONArray(List.of("a", "b")));

    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(value.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("a", "b");
  }

  @Test
  void aJsonObjectWithAnArrayIsAMapWithAList() {
    final GrpcValue value = GrpcTypeConverter.toGrpcValue(new JSONObject().put("tags", new JSONArray(List.of(1, 2))));

    assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.MAP_VALUE);
    final GrpcValue tags = value.getMapValue().getEntriesOrThrow("tags");
    assertThat(tags.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(tags.getListValue().getValuesList()).extracting(GrpcValue::getInt32Value).containsExactly(1, 2);
  }

  /** A Set, the other Collection a property holds, was a string too: the converter's only container arm was List. */
  @Test
  void aSetAndAnIteratorAreLists() {
    final GrpcValue set = GrpcTypeConverter.toGrpcValue(new LinkedHashSet<>(List.of("x", "y")));
    final GrpcValue iterator = GrpcTypeConverter.toGrpcValue(List.of(3L, 4L).iterator());

    assertThat(set.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("x", "y");
    assertThat(iterator.getListValue().getValuesList()).extracting(GrpcValue::getInt64Value).containsExactly(3L, 4L);
  }

  @Test
  void theTextKindsStayText() {
    final UUID uuid = UUID.randomUUID();

    assertThat(GrpcTypeConverter.toGrpcValue('c').getStringValue()).isEqualTo("c");
    assertThat(GrpcTypeConverter.toGrpcValue(uuid).getStringValue()).isEqualTo(uuid.toString());
    assertThat(GrpcTypeConverter.toGrpcValue(Type.STRING).getStringValue()).isEqualTo(String.valueOf(Type.STRING));
    assertThat(GrpcTypeConverter.toGrpcValue(null).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }

  /** The reported path end to end: an array property of a full-text hit, through the RPC's own conversion of the hits. */
  @Test
  void anArrayPropertyOfAFullTextHitIsAList() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    final Database database = new DatabaseFactory(DB_PATH).create();
    try {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Article");
        type.createProperty("text", Type.STRING);
        type.createProperty("tags", Type.LIST);
        type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "text");
        database.newDocument("Article").set("text", "graph databases").set("tags", List.of("graph", "db")).save();
      });

      final FullTextSearchResponse response = GrpcVectorSearch.fullTextSearch(database,
          FullTextSearchRequest.newBuilder().setQueryText("graph").setTypeName("Article").build());

      assertThat(response.getResultsCount()).isEqualTo(1);
      final GrpcValue tags = response.getResults(0).getRecord().getPropertiesOrThrow("tags");
      assertThat(tags.getKindCase()).as("the same kind the query path sends for the property").isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      assertThat(tags.getListValue().getValuesList()).extracting(GrpcValue::getStringValue).containsExactly("graph", "db");
    } finally {
      database.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  /*
   * Issue #7694, gRPC half - the fourth reader of {@code MultiColumnAggregationResult.getValue()}, and the one
   * issue #7653 found had no regression coverage at all.
   * <p>
   * The other three readers are pinned: {@code TimeSeriesAbsentSampleEncodingTest} and
   * {@code TimeSeriesHttpIT} for the two HTTP time-series handlers,
   * {@code TimeSeriesNaNHandlingTest} for {@code AggregateFromTimeSeriesStep}. The gRPC aggregation
   * stream in {@code ArcadeDbGrpcService} hands every value to {@link GrpcTimeSeriesSupport#toSampleValue}, which
   * maps an absent sample to an unset {@code GrpcValue} - correct by inspection, and until now driven by nothing, so
   * "simplifying" it to a bare {@code setDoubleValue} would have shipped green and a client would have read a gap as
   * a measurement.
   * <p>
   * This drives the REAL accumulator into the REAL encoder, the same composition the RPC performs
   * ({@code toSampleValue(result.getWireValue(timestamp, r))}), so it pins the contract rather than the mapper in
   * isolation, and without needing a bound gRPC port.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7694">issue #7694</a>
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7653">issue #7653</a>
   */
  private static final long BUCKET = 1_000L;

  private static List<MultiColumnAggregationRequest> sumAvgCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
  }

  /** The encoding loop of {@code ArcadeDbGrpcService.timeSeriesQuery}'s aggregation branch, verbatim. */
  private static List<GrpcValue> encode(final MultiColumnAggregationResult result) {
    final TimeSeriesBucket.Builder bucket = TimeSeriesBucket.newBuilder().setTimestamp(BUCKET);
    for (int r = 0; r < 3; r++)
      bucket.addValues(GrpcTimeSeriesSupport.toSampleValue(result.getWireValue(BUCKET, r)));
    return bucket.build().getValuesList();
  }

  // Regression: #7694
  /**
   * A bucket whose every sample was absent: SUM and AVG must reach the client as an UNSET {@code GrpcValue} - the
   * proto's own null - and COUNT, the one aggregate outside the policy, as the number of rows it really saw.
   */
  @Test
  void anAbsentSumAndAvgAreEncodedAsAnUnsetGrpcValue() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.finalizeAvg();

    assertThat(result.getValue(BUCKET, 0)).as("the accumulator itself answers absent").isNaN();
    assertThat(result.getValue(BUCKET, 1)).isNaN();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).as("an absent SUM must arrive unset, never as a double")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(1).getKindCase()).as("an absent AVG must arrive unset, never as a double")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getKindCase()).as("COUNT counts rows and is never absent")
        .isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(2).getDoubleValue()).isEqualTo(2.0);
  }

  // Regression: #7694
  /**
   * The unset value is the absent marker, not a blanket rule: real samples that happen to cancel to zero still
   * arrive as the number {@code 0}. This is the distinction that makes the unset above mean something.
   */
  @Test
  void aRealTotalOfZeroIsStillTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 2.5, 2.5, 2.5 });
    result.accumulateRow(BUCKET, new double[] { -2.5, -2.5, -2.5 });
    result.finalizeAvg();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).as("a real total of zero is data, not a gap")
        .isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(0).getDoubleValue()).isEqualTo(0.0);
    // getDoubleValue() answers 0.0 for an UNSET oneof too, so the kind has to be asserted first or this pair
    // would pass against the very regression the class exists to catch (CodeRabbit on PR #7718).
    assertThat(values.get(1).getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(1).getDoubleValue()).isEqualTo(0.0);
  }

  // Regression: #7694
  /** Absent samples among real ones are skipped rather than propagated, all the way to the wire value. */
  @Test
  void absentSamplesAreSkippedRatherThanPoisoningTheTotal() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 4.0, 4.0, 4.0 });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { 6.0, 6.0, 6.0 });
    result.finalizeAvg();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getDoubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(values.get(1).getDoubleValue()).as("AVG divides by the 2 real samples, not by 3").isEqualTo(5.0);
  }

  // Regression: #7694
  /**
   * A SUM request in a bucket a sibling request brought into existence: the state issue #7506 made answer zero and
   * issue #7694 decided answers absent. Over gRPC the difference is a client reading a total of {@code 0} where the
   * series has no data at all.
   */
  @Test
  void aSumRequestNoSampleReachedInAnExistingBucketAlsoArrivesUnset() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    // Only the COUNT request is offered a sample; SUM and AVG over the same column stay empty.
    result.accumulate(BUCKET, 2, 7.0);
    result.finalizeAvg();

    assertThat(result.getBucketTimestamps()).as("the bucket exists, so the RPC does emit it").containsExactly(BUCKET);

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(1).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getDoubleValue()).as("COUNT is real").isEqualTo(1.0);
  }

  // Regression: #7694
  /**
   * The failure mode the encoder exists to prevent, pinned so nobody "simplifies" {@code toSampleValue} into the
   * plain converter: that one sets the double field, and a NaN in a set field is a number as far as a client's
   * generated accessor is concerned.
   */
  @Test
  void thePlainConverterIsWhatWouldTurnTheGapIntoASetDouble() {
    final GrpcValue raw = GrpcTypeConverter.toGrpcValue(Double.NaN);

    assertThat(raw.getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(Double.isNaN(raw.getDoubleValue())).isTrue();
  }
}
