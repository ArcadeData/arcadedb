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
package com.arcadedb.remote.grpc;

import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetResultsSamples;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.schema.Type;
import com.arcadedb.server.grpc.FacetProtocol;
import com.arcadedb.server.grpc.GrpcFacetBucket;
import com.arcadedb.server.grpc.GrpcFacetResult;
import com.arcadedb.server.grpc.GrpcFacets;
import com.arcadedb.server.grpc.GrpcValue;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link FacetProtocol} carries {@link FacetResults} over gRPC without changing their JSON, the single shape of every transport. */
class FacetProtocolTest {
  /** As the server encodes the values of its rows. */
  private static GrpcFacets toProto(final FacetResults facets) {
    return FacetProtocol.toProto(facets, ProtoUtils::toGrpcValue);
  }

  /** As the client decodes the values of its rows. */
  private static FacetResults fromProto(final GrpcFacets proto) {
    return FacetProtocol.fromProto(proto, ProtoUtils::fromGrpcValue);
  }

  /**
   * Final review C-I8: a date term, a decimal, a double and NaN travel as the values of a row do (a timestamp, a decimal, a double) and
   * come back as the row decoder gives them. The engine hands the terms over in the Java type a row of the property holds (a Double
   * for a DOUBLE property): the protocol converts nothing.
   */
  @Test
  void valuesTravelAsRowValues() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("day", FacetResult.of("terms", "index", List.of(FacetBucket.term(LocalDate.of(2024, 2, 29), 3L)), 0L, 0L)
        .withValueType(Type.DATE));
    map.put("at", FacetResult.of("terms", "index", List.of(FacetBucket.term(LocalDateTime.of(2024, 2, 29, 10, 30), 2L)), 0L, 0L)
        .withValueType(Type.DATETIME));
    map.put("price", FacetResult.of("terms", "scan", List.of(FacetBucket.term(new BigDecimal("12.50"), 1L)), 0L, 0L)
        .withValueType(Type.DECIMAL));
    map.put("score", FacetResult.of("terms", "index", List.of(FacetBucket.term(1.25d, 4L),
        FacetBucket.term(Double.NaN, 1L)), 0L, 0L).withValueType(Type.DOUBLE));
    final GrpcFacets proto = toProto(new FacetResults(true, 0L, 0L, map));

    assertThat(proto.getResultsOrThrow("day").getBuckets(0).getValue().getKindCase()).isEqualTo(GrpcValue.KindCase.TIMESTAMP_VALUE);
    assertThat(proto.getResultsOrThrow("price").getBuckets(0).getValue().getKindCase()).isEqualTo(GrpcValue.KindCase.DECIMAL_VALUE);
    assertThat(proto.getResultsOrThrow("score").getBuckets(0).getValue().getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);

    final FacetResults back = fromProto(proto);
    assertThat(back.get("day").buckets().get(0).value()).isEqualTo(LocalDate.of(2024, 2, 29));
    assertThat(back.get("at").buckets().get(0).value()).isEqualTo(LocalDateTime.of(2024, 2, 29, 10, 30));
    assertThat(back.get("price").buckets().get(0).value()).isEqualTo(new BigDecimal("12.50"));
    assertThat(back.get("score").buckets().get(0).value()).isEqualTo(1.25d);
    assertThat(back.get("score").buckets().get(1).value()).isEqualTo(Double.NaN);
  }

  @Test
  void roundTripEqualsJson() {
    final FacetResults sample = FacetResultsSamples.allKinds();
    assertThat(fromProto(toProto(sample)).toJSON().toString()).isEqualTo(sample.toJSON().toString());
  }

  @Test
  void plan5MembersRoundTrip() {
    final FacetResults sample = FacetResultsSamples.plan5Members();
    assertThat(fromProto(toProto(sample)).toJSON().toString()).isEqualTo(sample.toJSON().toString());
  }

  @Test
  void roundTripSurvivesTheWireBytes() throws Exception {
    final FacetResults sample = FacetResultsSamples.allKinds();
    final GrpcFacets parsed = GrpcFacets.parseFrom(toProto(sample).toByteArray());
    assertThat(fromProto(parsed).toJSON().toString()).isEqualTo(sample.toJSON().toString());
  }

  @Test
  void protoCarriesTypedFields() {
    final GrpcFacets proto = toProto(FacetResultsSamples.allKinds());

    assertThat(proto.getExact()).isTrue();
    assertThat(proto.getPatched()).isEqualTo(12L);
    assertThat(proto.getLagMs()).isEqualTo(340L);
    assertThat(proto.getAliasesList()).containsExactly("brand", "size", "pub", "gone");

    final GrpcFacetResult brand = proto.getResultsOrThrow("brand");
    assertThat(brand.getType()).isEqualTo("terms");
    assertThat(brand.getSource()).isEqualTo("index");
    assertThat(brand.getHasCounts()).isTrue();
    assertThat(brand.getOtherCount()).isEqualTo(55L);
    assertThat(brand.getMissing()).isEqualTo(9L);
    assertThat(brand.getExtensionsJson()).isEmpty();
    assertThat(brand.getBucketsList()).hasSize(4);
    assertThat(brand.getBuckets(0).getValue().getStringValue()).isEqualTo("Nike");
    assertThat(brand.getBuckets(0).getCount()).isEqualTo(1240L);
    assertThat(brand.getBuckets(1).getValue().getKindCase()).isEqualTo(GrpcValue.KindCase.INT64_VALUE);
    assertThat(brand.getBuckets(1).getValue().getInt64Value()).isEqualTo(7L);
    assertThat(brand.getBuckets(2).getValue().getDoubleValue()).isEqualTo(1.5);
    final GrpcFacetBucket missing = brand.getBuckets(3);
    assertThat(missing.getMissing()).isTrue();
    assertThat(missing.hasValue()).isFalse();
    assertThat(missing.getCount()).isEqualTo(4L);

    final GrpcFacetResult size = proto.getResultsOrThrow("size");
    final GrpcFacetBucket small = size.getBuckets(0);
    assertThat(small.getKey()).isEqualTo("small");
    assertThat(small.hasFrom()).isFalse();
    assertThat(small.getTo().getInt64Value()).isEqualTo(1024L);
    assertThat(small.hasValue()).isFalse();
    assertThat(size.getBuckets(2).getIsDefault()).isTrue();
    assertThat(size.getBuckets(2).getKey()).isEqualTo("other");

    final GrpcFacetResult gone = proto.getResultsOrThrow("gone");
    assertThat(gone.getSource()).isEqualTo("unavailable");
    assertThat(gone.getReason()).isEqualTo("memory budget");
    assertThat(gone.getHasCounts()).isFalse();
    assertThat(gone.getBucketsList()).isEmpty();
  }

  @Test
  void plan5MembersTravelAsJsonText() {
    final GrpcFacets proto = toProto(FacetResultsSamples.plan5Members());
    assertThat(proto.getExact()).isFalse();
    assertThat(proto.getResultsOrThrow("tree").getBuckets(0).getAttributesJson()).contains("\"parent\"").contains("\"depth\"");
    assertThat(proto.getResultsOrThrow("uniq").getExtensionsJson()).contains("\"value\"").contains("\"exact\"");
    assertThat(proto.getResultsOrThrow("st").getExtensionsJson()).contains("\"avg\"");
    assertThat(proto.getResultsOrThrow("last").getExtensionsJson()).contains("\"after\"");
  }

  /** An empty range key and DEFAULT label stay range buckets: the key field has presence, so "" is not taken for "no key". */
  @Test
  void emptyRangeKeyAndDefaultLabelRoundTrip() throws Exception {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("size", FacetResult.of("range", "index", List.of(FacetBucket.range("", 0L, 10L, 4L), FacetBucket.rangeDefault("", 1L)), 0L, 0L));
    final FacetResults sample = new FacetResults(true, 0L, 0L, map);

    final FacetResults back = fromProto(GrpcFacets.parseFrom(toProto(sample).toByteArray()));
    assertThat(back.toJSON().toString()).isEqualTo(sample.toJSON().toString());
    assertThat(back.get("size").buckets().get(0).key()).isEmpty();
    assertThat(back.get("size").buckets().get(1).isDefault()).isTrue();
  }

  /** A value kind this client does not know makes that one facet unavailable; the other facets, and the stream, survive. */
  @Test
  void unknownValueKindMakesOnlyThatFacetUnavailable() {
    final GrpcFacets proto = toProto(FacetResultsSamples.allKinds()).toBuilder()
        .putResults("brand", GrpcFacetResult.newBuilder().setType("terms").setSource("index").setHasCounts(true)
            .addBuckets(GrpcFacetBucket.newBuilder().setValue(GrpcValue.newBuilder().setBytesValue(ByteString.copyFromUtf8("x"))).setCount(1L))
            .build())
        .build();

    final FacetResults back = fromProto(proto);
    assertThat(back.aliases()).containsExactly("brand", "size", "pub", "gone");
    assertThat(back.get("brand").source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(back.get("brand").kind()).isEqualTo("terms");
    assertThat(back.get("brand").reason()).isEqualTo("unsupported value kind on the wire: BYTES_VALUE");
    assertThat(back.get("size").toJSON().toString()).isEqualTo(FacetResultsSamples.allKinds().get("size").toJSON().toString());
  }

  @Test
  void emptyResultsRoundTrip() {
    final FacetResults empty = new FacetResults(true, 0L, 0L, new LinkedHashMap<>());
    assertThat(fromProto(toProto(empty)).toJSON().toString()).isEqualTo(empty.toJSON().toString());
  }

  @Test
  void decimalAndBooleanValuesRoundTrip() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("price", FacetResult.of("terms", "scan",
        List.of(FacetBucket.term(new BigDecimal("12345678901234567890.123456789"), 2L), FacetBucket.term(true, 1L)), 0L, 0L));
    final FacetResults sample = new FacetResults(true, 0L, 0L, map);

    final FacetResults back = fromProto(toProto(sample));
    assertThat(back.toJSON().toString()).isEqualTo(sample.toJSON().toString());
    assertThat(back.get("price").buckets().get(0).value()).isEqualTo(new BigDecimal("12345678901234567890.123456789"));
    assertThat(back.get("price").buckets().get(1).value()).isEqualTo(Boolean.TRUE);
  }
}
