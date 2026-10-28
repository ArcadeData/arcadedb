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

import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.util.LinkedHashMap;

/**
 * Converts {@link FacetResults} to and from {@link GrpcFacets}. Both directions go through the JSON of every other transport: each
 * result is read from {@link FacetResult#toJSON()} and rebuilt with {@link FacetResult#fromJSON}, so
 * {@code fromProto(toProto(x)).toJSON()} equals {@code x.toJSON()} and gRPC invents no shape of its own. The members of a bucket or
 * result that have no proto field (bucket attributes, the cursor, the DISTINCT and STATS members) travel as JSON object text.
 * <p>
 * Values are the scalars the facet JSON holds: null (an unset {@link GrpcValue}), String, integral numbers as {@code int64}, other
 * numbers as {@code double}, Boolean, and a {@link BigDecimal} as its text with the logical type {@code decimal}. A received value of
 * any other kind (a newer server) makes that one facet {@code unavailable} with the reason naming the kind, so the response or the
 * stream that carries the facets is still read to the end.
 */
public final class FacetProtocol {
  private static final String TYPE        = "type";
  private static final String SOURCE      = "source";
  private static final String REASON      = "reason";
  private static final String BUCKETS     = "buckets";
  private static final String OTHER_COUNT = "otherCount";
  private static final String MISSING     = "missing";
  private static final String VALUE       = "value";
  private static final String KEY         = "key";
  private static final String FROM        = "from";
  private static final String TO          = "to";
  private static final String COUNT       = "count";
  private static final String DEFAULT     = "default";
  private static final String DECIMAL     = "decimal";

  private FacetProtocol() {
  }

  public static GrpcFacets toProto(final FacetResults facets) {
    final GrpcFacets.Builder out = GrpcFacets.newBuilder().setExact(facets.exact()).setPatched(facets.patched()).setLagMs(facets.lagMs());
    for (final String alias : facets.aliases()) {
      out.addAliases(alias);
      out.putResults(alias, resultToProto(facets.get(alias).toJSON()));
    }
    return out.build();
  }

  public static FacetResults fromProto(final GrpcFacets proto) {
    final LinkedHashMap<String, FacetResult> results = new LinkedHashMap<>();
    for (final String alias : proto.getAliasesList()) {
      final GrpcFacetResult result = proto.getResultsMap().get(alias);
      if (result == null)
        throw new IllegalArgumentException("Facet alias '" + alias + "' has no result");
      FacetResult facet;
      try {
        facet = FacetResult.fromJSON(resultFromProto(result));
      } catch (final UnsupportedValueKindException e) {
        // a newer server may send a value kind this client does not know: losing that facet beats failing the stream it rides on
        facet = FacetResult.unavailable(result.getType(), e.getMessage());
      }
      results.put(alias, facet);
    }
    return new FacetResults(proto.getExact(), proto.getPatched(), proto.getLagMs(), results);
  }

  /** A {@link GrpcValue} kind outside the scalar domain of facet values; {@link #fromProto} turns it into an unavailable facet. */
  private static final class UnsupportedValueKindException extends RuntimeException {
    private UnsupportedValueKindException(final GrpcValue.KindCase kind) {
      super("unsupported value kind on the wire: " + kind);
    }
  }

  private static GrpcFacetResult resultToProto(final JSONObject json) {
    final GrpcFacetResult.Builder out = GrpcFacetResult.newBuilder().setType(json.getString(TYPE)).setSource(json.getString(SOURCE));
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
        final JSONArray buckets = json.getJSONArray(BUCKETS);
        for (int i = 0; i < buckets.length(); i++)
          out.addBuckets(bucketToProto(buckets.getJSONObject(i)));
      }
      case OTHER_COUNT -> out.setOtherCount(json.getLong(OTHER_COUNT));
      case MISSING -> out.setMissing(json.getLong(MISSING)).setHasCounts(true);
      default -> extensions.put(name, json.opt(name));
      }
    if (!extensions.isEmpty())
      out.setExtensionsJson(extensions.toString());
    return out.build();
  }

  private static JSONObject resultFromProto(final GrpcFacetResult proto) {
    final JSONObject json = new JSONObject().put(TYPE, proto.getType()).put(SOURCE, proto.getSource());
    if (proto.hasReason())
      json.put(REASON, proto.getReason());
    if (proto.getHasCounts()) {
      final JSONArray buckets = new JSONArray();
      for (final GrpcFacetBucket bucket : proto.getBucketsList())
        buckets.put(bucketFromProto(bucket));
      json.put(BUCKETS, buckets).put(OTHER_COUNT, proto.getOtherCount()).put(MISSING, proto.getMissing());
    }
    merge(json, proto.getExtensionsJson());
    return json;
  }

  private static GrpcFacetBucket bucketToProto(final JSONObject json) {
    final GrpcFacetBucket.Builder out = GrpcFacetBucket.newBuilder();
    final JSONObject attributes = new JSONObject();
    for (final String name : json.keySet())
      switch (name) {
      case VALUE -> {
        final GrpcValue value = toValue(json.opt(VALUE));
        if (value != null)
          out.setValue(value);
      }
      case KEY -> out.setKey(json.getString(KEY));
      case FROM -> {
        final GrpcValue from = toValue(json.opt(FROM));
        if (from != null)
          out.setFrom(from);
      }
      case TO -> {
        final GrpcValue to = toValue(json.opt(TO));
        if (to != null)
          out.setTo(to);
      }
      case COUNT -> out.setCount(json.getLong(COUNT));
      case MISSING -> out.setMissing(json.getBoolean(MISSING));
      case DEFAULT -> out.setIsDefault(json.getBoolean(DEFAULT));
      default -> attributes.put(name, json.opt(name));
      }
    if (!attributes.isEmpty())
      out.setAttributesJson(attributes.toString());
    return out.build();
  }

  private static JSONObject bucketFromProto(final GrpcFacetBucket proto) {
    final JSONObject json = new JSONObject();
    if (proto.hasKey()) {
      json.put(KEY, proto.getKey());
      if (proto.hasFrom())
        json.put(FROM, fromValue(proto.getFrom()));
      if (proto.hasTo())
        json.put(TO, fromValue(proto.getTo()));
    } else
      json.put(VALUE, proto.hasValue() ? fromValue(proto.getValue()) : null);
    json.put(COUNT, proto.getCount());
    if (proto.getMissing())
      json.put(MISSING, true);
    if (proto.getIsDefault())
      json.put(DEFAULT, true);
    merge(json, proto.getAttributesJson());
    return json;
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

  /**
   * The value as a {@link GrpcValue}; null for a null value, which travels as an unset field. Values come out of
   * {@link FacetResult#toJSON()}: the engine normalized them (a finite Float is a Long or BigDecimal by then, a non-finite number
   * is JSON null) and the bucket wrote Integer/Short/Byte as Long, so the narrower integral cases are defensive only.
   */
  private static GrpcValue toValue(final Object value) {
    return switch (value) {
      case null -> null;
      case String string -> GrpcValue.newBuilder().setStringValue(string).build();
      case Boolean bool -> GrpcValue.newBuilder().setBoolValue(bool).build();
      case Long l -> GrpcValue.newBuilder().setInt64Value(l).build();
      case Integer i -> GrpcValue.newBuilder().setInt64Value(i).build();
      case Short s -> GrpcValue.newBuilder().setInt64Value(s).build();
      case Byte b -> GrpcValue.newBuilder().setInt64Value(b).build();
      case BigDecimal decimal -> GrpcValue.newBuilder().setStringValue(decimal.toString()).setLogicalType(DECIMAL).build();
      case Number number -> GrpcValue.newBuilder().setDoubleValue(number.doubleValue()).build();
      default -> GrpcValue.newBuilder().setStringValue(value.toString()).build();
    };
  }

  /** @throws UnsupportedValueKindException for a kind outside the scalar domain (a list, a map, bytes, a timestamp, ...) */
  private static Object fromValue(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case KIND_NOT_SET -> null;
      case STRING_VALUE -> DECIMAL.equals(value.getLogicalType()) ? new BigDecimal(value.getStringValue()) : value.getStringValue();
      case BOOL_VALUE -> value.getBoolValue();
      case INT64_VALUE -> value.getInt64Value();
      case INT32_VALUE -> (long) value.getInt32Value();
      case DOUBLE_VALUE -> value.getDoubleValue();
      case FLOAT_VALUE -> (double) value.getFloatValue();
      default -> throw new UnsupportedValueKindException(value.getKindCase());
    };
  }
}
