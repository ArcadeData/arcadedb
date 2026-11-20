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
package com.arcadedb.query.opencypher.procedures.meta;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The optional {@code config} map of the {@code meta.*} procedures, with APOC's keys and defaults: {@code sample} (1000,
 * -1 reads every record), {@code maxRels} (100 relationships read per sampled node), {@code includeLabels} /
 * {@code excludeLabels} / {@code includeRels} / {@code excludeRels}, and the deprecated {@code labels} / {@code rels} /
 * {@code excludes} spellings of the first, third and second. Unknown keys are ignored, as APOC ignores them.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MetaConfig {
  static final long DEFAULT_SAMPLE   = 1000L;
  static final long DEFAULT_MAX_RELS = 100L;

  static final MetaConfig DEFAULT = new MetaConfig(Map.of());

  private final long        sample;
  private final long        maxRels;
  private final Set<String> includeLabels;
  private final Set<String> excludeLabels;
  private final Set<String> includeRels;
  private final Set<String> excludeRels;

  MetaConfig(final Map<String, Object> config) {
    this.sample = number(config.get("sample"), DEFAULT_SAMPLE);
    this.maxRels = number(config.get("maxRels"), DEFAULT_MAX_RELS);
    this.includeLabels = names(config, "includeLabels", "labels");
    this.excludeLabels = names(config, "excludeLabels", "excludes");
    this.includeRels = names(config, "includeRels", "rels");
    this.excludeRels = names(config, "excludeRels", null);
  }

  boolean acceptsLabel(final String label) {
    return (includeLabels.isEmpty() || includeLabels.contains(label)) && !excludeLabels.contains(label);
  }

  boolean acceptsRelType(final String type) {
    return (includeRels.isEmpty() || includeRels.contains(type)) && !excludeRels.contains(type);
  }

  /**
   * How many relationships of one node are read, unlimited for a value below 1.
   */
  long maxRels() {
    return maxRels < 1L ? Long.MAX_VALUE : maxRels;
  }

  /**
   * Every how many records of a label of {@code count} records one is examined: {@code count / sample} as APOC computes
   * it, without APOC's random jitter so that the same data answers the same rows. 1 (every record) when sampling is off
   * ({@code sample} below 1) or the label has no more records than the sample.
   */
  long skipFor(final long count) {
    if (sample < 1L || count <= sample)
      return 1L;
    return Math.max(1L, count / sample);
  }

  private static long number(final Object value, final long defaultValue) {
    if (value == null)
      return defaultValue;
    if (value instanceof Number number)
      return number.longValue();
    throw new IllegalArgumentException("meta config: expected a number, got " + value.getClass().getSimpleName());
  }

  private static Set<String> names(final Map<String, Object> config, final String key, final String deprecatedKey) {
    final Set<String> names = new HashSet<>();
    addNames(names, config.get(key));
    if (deprecatedKey != null)
      addNames(names, config.get(deprecatedKey));
    return names;
  }

  private static void addNames(final Set<String> target, final Object value) {
    if (value instanceof String name)
      target.add(name);
    else if (value instanceof Collection<?> collection)
      for (final Object item : collection)
        if (item != null)
          target.add(item.toString());
  }
}
