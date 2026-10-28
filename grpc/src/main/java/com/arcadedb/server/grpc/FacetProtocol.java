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

import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

/**
 * Converts {@link FacetResults} to and from {@link GrpcFacets}. The shape is the JSON of every other transport: each result's members
 * are those of {@link FacetResult#toJSON()}, and the members that have no proto field (bucket attributes, the DISTINCT members, the
 * counts, sum and average of a STATS facet) travel as JSON object text.
 * <p>
 * The values of the data (a term, a bound, the cursor, a STATS facet's min and max) are {@link GrpcValue}s encoded exactly as the
 * rows of the same transport encode theirs (final review C-I8): the server hands its row encoder, a client its row decoder, so a date term arrives as the temporal a row's date
 * does, a decimal as the {@code BigDecimal}, NaN as NaN (the engine already gives every term the Java type a row of its property
 * holds). A received value of a kind no facet value takes (a list, a map, bytes, a link: a newer server)
 * makes that one facet {@code unavailable} with the reason naming the kind, so the response or the stream that carries the facets is
 * still read to the end.
 */
public final class FacetProtocol {
  private static final String TYPE        = "type";
  private static final String SOURCE      = "source";
  private static final String REASON      = "reason";
  private static final String BUCKETS     = "buckets";
  private static final String OTHER_COUNT = "otherCount";
  private static final String MISSING     = "missing";
  private static final String AFTER       = "after";
  private static final String MIN         = "min";
  private static final String MAX         = "max";

  private FacetProtocol() {
  }

  /** @param encoder the encoder of the values of a row on this transport */
  public static GrpcFacets toProto(final FacetResults facets, final Function<Object, GrpcValue> encoder) {
    final GrpcFacets.Builder out = GrpcFacets.newBuilder().setExact(facets.exact()).setPatched(facets.patched()).setLagMs(facets.lagMs());
    for (final String alias : facets.aliases()) {
      out.addAliases(alias);
      out.putResults(alias, resultToProto(facets.get(alias), encoder));
    }
    return out.build();
  }

  /** @param decoder the decoder of the values of a row on this transport */
  public static FacetResults fromProto(final GrpcFacets proto, final Function<GrpcValue, Object> decoder) {
    final LinkedHashMap<String, FacetResult> results = new LinkedHashMap<>();
    for (final String alias : proto.getAliasesList()) {
      final GrpcFacetResult result = proto.getResultsMap().get(alias);
      if (result == null)
        throw new IllegalArgumentException("Facet alias '" + alias + "' has no result");
      FacetResult facet;
      try {
        facet = resultFromProto(result, decoder);
      } catch (final UnsupportedValueKindException e) {
        // a newer server may send a value kind this client does not know: losing that facet beats failing the stream it rides on
        facet = FacetResult.unavailable(result.getType(), e.getMessage());
      }
      results.put(alias, facet);
    }
    return new FacetResults(proto.getExact(), proto.getPatched(), proto.getLagMs(), results);
  }

  /** A {@link GrpcValue} kind outside the domain of facet values; {@link #fromProto} turns it into an unavailable facet. */
  private static final class UnsupportedValueKindException extends RuntimeException {
    private UnsupportedValueKindException(final GrpcValue.KindCase kind) {
      super("unsupported value kind on the wire: " + kind);
    }
  }

  private static GrpcFacetResult resultToProto(final FacetResult result, final Function<Object, GrpcValue> encoder) {
    final JSONObject json = result.toJSON();
    final GrpcFacetResult.Builder out = GrpcFacetResult.newBuilder().setType(result.kind()).setSource(result.source());
    final JSONObject extensions = new JSONObject();
    for (final String name : json.keySet())
      switch (name) {
      case TYPE, SOURCE -> {
      }
      case REASON -> {
        if (!json.isNull(REASON))
          out.setReason(json.getString(REASON));
      }
      case BUCKETS -> {
        for (final FacetBucket bucket : result.buckets())
          out.addBuckets(bucketToProto(bucket, encoder));
      }
      case OTHER_COUNT -> out.setOtherCount(json.getLong(OTHER_COUNT));
      case MISSING -> out.setMissing(json.getLong(MISSING)).setHasCounts(true);
      // typed below: values of the property
      case AFTER, MIN, MAX -> {
      }
      default -> extensions.put(name, json.opt(name));
      }
    if (!extensions.isEmpty())
      out.setExtensionsJson(extensions.toString());
    final FacetStats stats = result.stats();
    if (stats != null && !FacetResult.UNAVAILABLE.equals(result.source())) {
      if (stats.min() != null)
        out.setMin(encoder.apply(stats.min()));
      if (stats.max() != null)
        out.setMax(encoder.apply(stats.max()));
    }
    if (result.hasAfter()) {
      out.setHasAfter(true);
      if (result.after() != null)
        out.setAfter(encoder.apply(result.after()));
    }
    return out.build();
  }

  private static FacetResult resultFromProto(final GrpcFacetResult proto, final Function<GrpcValue, Object> decoder) {
    final JSONObject json = new JSONObject().put(TYPE, proto.getType()).put(SOURCE, proto.getSource());
    if (proto.hasReason())
      json.put(REASON, proto.getReason());
    final List<FacetBucket> buckets = new ArrayList<>(proto.getBucketsCount());
    if (proto.getHasCounts()) {
      for (final GrpcFacetBucket bucket : proto.getBucketsList())
        buckets.add(bucketFromProto(bucket, decoder));
      // the buckets are rebuilt from their typed values below: the JSON carries the counts and the other members only
      json.put(BUCKETS, new JSONArray()).put(OTHER_COUNT, proto.getOtherCount()).put(MISSING, proto.getMissing());
    }
    merge(json, proto.getExtensionsJson());
    FacetResult result = FacetResult.fromJSON(json);
    if (proto.getHasCounts())
      result = result.withBuckets(buckets);
    final FacetStats stats = result.stats();
    if (stats != null && (proto.hasMin() || proto.hasMax()))
      result = FacetResult.ofStats(result.source(), new FacetStats(stats.count(), stats.missing(),
          proto.hasMin() ? decode(proto.getMin(), decoder) : null, proto.hasMax() ? decode(proto.getMax(), decoder) : null, stats.sum(),
          stats.avg()));
    // a server without the typed cursor sends it in the extensions, which fromJSON has read already
    if (proto.getHasAfter())
      result = result.withAfter(proto.hasAfter() ? decode(proto.getAfter(), decoder) : null);
    return result;
  }

  private static GrpcFacetBucket bucketToProto(final FacetBucket bucket, final Function<Object, GrpcValue> encoder) {
    final GrpcFacetBucket.Builder out = GrpcFacetBucket.newBuilder().setCount(bucket.count());
    if (bucket.key() != null) {
      out.setKey(bucket.key());
      if (bucket.isDefault())
        out.setIsDefault(true);
      else {
        if (bucket.from() != null)
          out.setFrom(encoder.apply(bucket.from()));
        if (bucket.to() != null)
          out.setTo(encoder.apply(bucket.to()));
      }
    } else if (bucket.isMissing())
      out.setMissing(true);
    else if (bucket.value() != null)
      out.setValue(encoder.apply(bucket.value()));
    final JSONObject attributes = attributesOf(bucket);
    if (!attributes.isEmpty())
      out.setAttributesJson(attributes.toString());
    return out.build();
  }

  /** The attributes of the bucket its JSON carries: a name equal to a member of the bucket itself is not one (see FacetBucket). */
  private static JSONObject attributesOf(final FacetBucket bucket) {
    final JSONObject own = bucket.attributes();
    final JSONObject attributes = new JSONObject();
    for (final String name : own.keySet())
      if (!isBucketMember(name))
        attributes.put(name, own.opt(name));
    return attributes;
  }

  private static boolean isBucketMember(final String name) {
    return switch (name) {
      case "value", "key", "from", "to", "count", "missing", "default" -> true;
      default -> false;
    };
  }

  private static FacetBucket bucketFromProto(final GrpcFacetBucket proto, final Function<GrpcValue, Object> decoder) {
    final long count = proto.getCount();
    final FacetBucket bucket;
    if (proto.getMissing())
      bucket = FacetBucket.missing(count);
    else if (proto.getIsDefault())
      bucket = FacetBucket.rangeDefault(proto.getKey(), count);
    else if (proto.hasKey())
      bucket = FacetBucket.range(proto.getKey(), proto.hasFrom() ? decode(proto.getFrom(), decoder) : null,
          proto.hasTo() ? decode(proto.getTo(), decoder) : null, count);
    else
      bucket = FacetBucket.term(proto.hasValue() ? decode(proto.getValue(), decoder) : null, count);
    if (!proto.getAttributesJson().isEmpty())
      bucket.withAttributes(new JSONObject(proto.getAttributesJson()));
    return bucket;
  }

  /** Adds the members of a JSON object text that are not already set; an empty text adds nothing. */
  private static void merge(final JSONObject target, final String jsonText) {
    if (jsonText.isEmpty())
      return;
    final JSONObject extra = new JSONObject(jsonText);
    for (final String name : extra.keySet())
      if (!target.has(name))
        target.put(name, extra.opt(name));
  }

  /** @throws UnsupportedValueKindException for a kind no facet value takes (a list, a map, bytes, a link, ...) */
  private static Object decode(final GrpcValue value, final Function<GrpcValue, Object> decoder) {
    return switch (value.getKindCase()) {
      case KIND_NOT_SET, STRING_VALUE, BOOL_VALUE, INT32_VALUE, INT64_VALUE, FLOAT_VALUE, DOUBLE_VALUE, TIMESTAMP_VALUE, DECIMAL_VALUE ->
          decoder.apply(value);
      default -> throw new UnsupportedValueKindException(value.getKindCase());
    };
  }
}
