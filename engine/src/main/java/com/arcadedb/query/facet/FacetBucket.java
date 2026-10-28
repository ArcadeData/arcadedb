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
package com.arcadedb.query.facet;

import com.arcadedb.serializer.json.JSONObject;

/**
 * One bucket of a facet: a value of a terms facet, the bucket of the records without a value, or a range or histogram interval.
 */
public final class FacetBucket {
  private static final String VALUE   = "value";
  private static final String KEY     = "key";
  private static final String FROM    = "from";
  private static final String TO      = "to";
  private static final String COUNT   = "count";
  private static final String MISSING = "missing";
  private static final String DEFAULT = "default";

  private final Object  value;
  private final String  key;
  private final Object  from;
  private final Object  to;
  private final long    count;
  private final boolean missing;
  private final boolean isDefault;
  private JSONObject    attributes;

  private FacetBucket(final Object value, final String key, final Object from, final Object to, final long count, final boolean missing,
      final boolean isDefault) {
    this.value = value;
    this.key = key;
    this.from = from;
    this.to = to;
    this.count = count;
    this.missing = missing;
    this.isDefault = isDefault;
  }

  public static FacetBucket term(final Object value, final long count) {
    return new FacetBucket(value, null, null, null, count, false, false);
  }

  /** The bucket of the records without a value. */
  public static FacetBucket missing(final long count) {
    return new FacetBucket(null, null, null, null, count, true, false);
  }

  /** A range bucket; a null {@code from} or {@code to} is an unbounded end. */
  public static FacetBucket range(final String key, final Object from, final Object to, final long count) {
    return new FacetBucket(null, key, from, to, count, false, false);
  }

  /** The bucket of the values in no range. */
  public static FacetBucket rangeDefault(final String key, final long count) {
    return new FacetBucket(null, key, null, null, count, false, true);
  }

  public static FacetBucket histogram(final String key, final Object from, final Object to, final long count) {
    return new FacetBucket(null, key, from, to, count, false, false);
  }

  public Object value() {
    return value;
  }

  public String key() {
    return key;
  }

  public Object from() {
    return from;
  }

  public Object to() {
    return to;
  }

  public long count() {
    return count;
  }

  public boolean isMissing() {
    return missing;
  }

  public boolean isDefault() {
    return isDefault;
  }

  /**
   * The extra members merged into the JSON of the bucket; a new empty object (detached from the bucket) unless set by
   * {@link #withAttributes}. A name equal to a member of the bucket itself (value, key, from, to, count, missing, default) is not
   * written to the JSON: the bucket's own member wins.
   */
  public JSONObject attributes() {
    return attributes != null ? attributes : new JSONObject();
  }

  /** Sets the extra members merged into the JSON of the bucket (null for none); returns this bucket. */
  public FacetBucket withAttributes(final JSONObject attributes) {
    this.attributes = attributes;
    return this;
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    if (key != null) {
      json.put(KEY, key);
      if (isDefault)
        json.put(DEFAULT, true);
      else {
        if (from != null)
          json.put(FROM, integral(from));
        if (to != null)
          json.put(TO, integral(to));
      }
    } else {
      json.put(VALUE, integral(value));
      if (missing)
        json.put(MISSING, true);
    }
    json.put(COUNT, count);
    if (attributes != null)
      for (final String name : attributes.keySet())
        if (!isKnownMember(name))
          json.put(name, attributes.get(name));
    return json;
  }

  public static FacetBucket fromJSON(final JSONObject json) {
    final long count = json.getLong(COUNT, 0L);
    final FacetBucket bucket;
    if (json.getBoolean(MISSING, false))
      bucket = missing(count);
    else if (json.getBoolean(DEFAULT, false))
      bucket = rangeDefault(json.getString(KEY), count);
    else if (json.has(KEY))
      bucket = range(json.getString(KEY), integral(json.opt(FROM)), integral(json.opt(TO)), count);
    else
      bucket = term(integral(json.opt(VALUE)), count);
    JSONObject extra = null;
    for (final String name : json.keySet())
      if (!isKnownMember(name)) {
        if (extra == null)
          extra = new JSONObject();
        extra.put(name, json.get(name));
      }
    bucket.attributes = extra;
    return bucket;
  }

  private static boolean isKnownMember(final String name) {
    return switch (name) {
      case VALUE, KEY, FROM, TO, COUNT, MISSING, DEFAULT -> true;
      default -> false;
    };
  }

  /** An integral number as a {@code Long}, anything else unchanged: the form JSON writes and the form it is read back as. */
  static Object integral(final Object value) {
    return value instanceof Integer || value instanceof Short || value instanceof Byte ? Long.valueOf(((Number) value).longValue()) : value;
  }
}
