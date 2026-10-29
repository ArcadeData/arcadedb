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
package com.arcadedb.test.support;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Seeded generator of the field values of a {@code FacetAsset}: the same seed yields the same sequence. Not thread-safe: use one
 * instance per writer thread.
 */
public final class AssetGenerator {
  public static final List<String> BRANDS;
  public static final List<String> TAGS;

  private static final long[] BOUNDARY_SIZES = { 1023L, 1024L, 1_048_575L, 1_048_576L };
  private static final long   SIZE_LIMIT     = 8L * 1024 * 1024;
  private static final int[]  GROWN_BODIES   = { 2 * 1024, 20 * 1024, 70 * 1024 };
  private static final String BODY           = "x".repeat(200);

  static {
    final List<String> brands = new ArrayList<>();
    for (int i = 0; i < 12; i++)
      brands.add("brand" + (i < 10 ? "0" : "") + i);
    BRANDS = List.copyOf(brands);
    final List<String> tags = new ArrayList<>();
    for (int i = 0; i < 64; i++)
      tags.add("tag" + (i < 10 ? "0" : "") + i);
    TAGS = List.copyOf(tags);
  }

  private final Random random;

  public AssetGenerator(final long seed) {
    this.random = new Random(seed);
  }

  /** @return the parameters of {@code INSERT}; {@code INSERT_NO_BRAND} ignores the brand */
  public Map<String, Object> insertParams(final long aid) {
    final Map<String, Object> params = valuesParams(aid);
    params.put("body", BODY);
    return params;
  }

  /** @return true for 5% of the calls: the caller then inserts the asset without a brand */
  public boolean omitBrand() {
    return random.nextInt(100) < 5;
  }

  /** @return the parameters of {@code UPDATE_VALUES} and {@code UPDATE_GROWN}: brand, 1-4 distinct tags and a file size */
  public Map<String, Object> valuesParams(final long aid) {
    final Map<String, Object> params = new HashMap<>();
    params.put("aid", aid);
    params.put("brand", BRANDS.get(random.nextInt(BRANDS.size())));
    params.put("tags", tags());
    params.put("fileSize", fileSize());
    return params;
  }

  /** @return the parameters of {@code GROW}: a body of 2 KB, 20 KB or 70 KB (more than a 64 KB page) and a brand */
  public Map<String, Object> growParams(final long aid) {
    final Map<String, Object> params = new HashMap<>();
    params.put("aid", aid);
    params.put("body", "y".repeat(GROWN_BODIES[random.nextInt(GROWN_BODIES.length)]));
    params.put("brand", BRANDS.get(random.nextInt(BRANDS.size())));
    return params;
  }

  /** @return the parameters of {@code REMOVE_BRAND} and {@code DELETE} */
  public Map<String, Object> keyParams(final long aid) {
    final Map<String, Object> params = new HashMap<>();
    params.put("aid", aid);
    return params;
  }

  /** @return the parameters of {@code LINK} */
  public Map<String, Object> linkParams(final long from, final long to) {
    final Map<String, Object> params = new HashMap<>();
    params.put("from", from);
    params.put("to", to);
    return params;
  }

  private List<String> tags() {
    final int count = 1 + random.nextInt(4);
    final List<String> tags = new ArrayList<>(count);
    while (tags.size() < count) {
      final String tag = TAGS.get(random.nextInt(TAGS.size()));
      if (!tags.contains(tag))
        tags.add(tag);
    }
    return tags;
  }

  private long fileSize() {
    if (random.nextInt(10) == 0)
      return BOUNDARY_SIZES[random.nextInt(BOUNDARY_SIZES.length)];
    // log-uniform over [0, 8 MB)
    return Math.min(SIZE_LIMIT - 1, (long) Math.exp(random.nextDouble() * Math.log(SIZE_LIMIT)) - 1);
  }
}
