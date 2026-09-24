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

package com.arcadedb.containers.ha.chaos;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable configuration of a chaos run, read from {@code chaos.*} system properties. The seed drives every random
 * decision of the run, so {@link #replayCommand()} reproduces the same fault sequence.
 */
public record ChaosConfig(long seed, int nodes, Duration duration, int maxSteps, int writers,
                          Map<String, Integer> faultWeights, Duration holdMin, Duration holdMax, Duration calmMin,
                          Duration calmMax, Duration convergenceTimeout, Duration electionTimeout,
                          Duration availabilityGrace, String nodeHeap) {

  private static final long    MIN_NODE_HEAP_BYTES = 256L << 20;
  private static final Pattern HEAP_SIZE           = Pattern.compile("(\\d+)([mMgG])");

  public static final List<String> ALL_FAULTS = List.of("kill", "stop", "rolling", "pause", "isolate", "split", "latency",
      "loss");

  public ChaosConfig {
    if (nodes != 3 && nodes != 5)
      throw new IllegalArgumentException("chaos.nodes must be 3 or 5, got " + nodes);
    if (writers < 1)
      throw new IllegalArgumentException("chaos.writers must be >= 1, got " + writers);
    if (maxSteps < 0)
      throw new IllegalArgumentException("chaos.maxSteps must be >= 0 (0 = unlimited), got " + maxSteps);
    if (duration.isNegative() || duration.isZero())
      throw new IllegalArgumentException("chaos.duration must be positive, got " + duration);
    requireRange("chaos.hold", holdMin, holdMax);
    requireRange("chaos.calm", calmMin, calmMax);
    if (faultWeights.isEmpty())
      throw new IllegalArgumentException("chaos.faults selects no fault");
    for (final String fault : faultWeights.keySet())
      if (!ALL_FAULTS.contains(fault))
        throw new IllegalArgumentException("Unknown fault '" + fault + "', valid faults: " + ALL_FAULTS);
    faultWeights = Collections.unmodifiableMap(new LinkedHashMap<>(faultWeights));
    if (heapBytes(nodeHeap) < MIN_NODE_HEAP_BYTES)
      throw new IllegalArgumentException("chaos.nodeHeap must be at least 256M, got " + nodeHeap);
  }

  public static ChaosConfig fromProperties(final Properties properties) {
    final String seedValue = properties.getProperty("chaos.seed", "").trim();
    final long seed = seedValue.isEmpty() ? ThreadLocalRandom.current().nextLong() : Long.parseLong(seedValue);
    return new ChaosConfig(seed,
        integer(properties, "chaos.nodes", 3),
        duration(properties, "chaos.duration", "PT20M"),
        integer(properties, "chaos.maxSteps", 0),
        integer(properties, "chaos.writers", 4),
        parseFaults(properties.getProperty("chaos.faults", "")),
        duration(properties, "chaos.holdMin", "PT10S"),
        duration(properties, "chaos.holdMax", "PT60S"),
        duration(properties, "chaos.calmMin", "PT10S"),
        duration(properties, "chaos.calmMax", "PT30S"),
        duration(properties, "chaos.convergenceTimeout", "PT2M"),
        duration(properties, "chaos.electionTimeout", "PT60S"),
        duration(properties, "chaos.availabilityGrace", "PT20S"),
        properties.getProperty("chaos.nodeHeap", "1G").trim());
  }

  /**
   * Parses {@code kill:3,pause} into an ordered name-to-weight map; a blank spec enables every fault with weight 1.
   */
  public static Map<String, Integer> parseFaults(final String spec) {
    final Map<String, Integer> weights = new LinkedHashMap<>();
    if (spec == null || spec.isBlank()) {
      for (final String fault : ALL_FAULTS)
        weights.put(fault, 1);
      return weights;
    }
    for (final String token : spec.split(",")) {
      final String trimmed = token.trim();
      if (trimmed.isEmpty())
        continue;
      final int colon = trimmed.indexOf(':');
      final String name = colon < 0 ? trimmed : trimmed.substring(0, colon).trim();
      final int weight = colon < 0 ? 1 : Integer.parseInt(trimmed.substring(colon + 1).trim());
      if (weight < 1)
        throw new IllegalArgumentException("Fault weight must be >= 1: '" + trimmed + "'");
      weights.merge(name, weight, Integer::sum);
    }
    return weights;
  }

  public String faultsSpec() {
    final StringJoiner joiner = new StringJoiner(",");
    faultWeights.forEach((name, weight) -> joiner.add(name + ":" + weight));
    return joiner.toString();
  }

  public String replayCommand() {
    return "./mvnw verify -Pintegration -pl e2e-ha -Dit.test=HaChaosIT -Dfailsafe.excludedGroups="
        + " -Dchaos.seed=" + seed
        + " -Dchaos.nodes=" + nodes
        + " -Dchaos.duration=" + duration
        + " -Dchaos.maxSteps=" + maxSteps
        + " -Dchaos.writers=" + writers
        + " -Dchaos.faults=" + faultsSpec()
        + " -Dchaos.holdMin=" + holdMin
        + " -Dchaos.holdMax=" + holdMax
        + " -Dchaos.calmMin=" + calmMin
        + " -Dchaos.calmMax=" + calmMax
        + " -Dchaos.nodeHeap=" + nodeHeap;
  }

  /** Heap of each ArcadeDB node in bytes; the container limit is twice this, for direct memory and page cache. */
  public long nodeHeapBytes() {
    return heapBytes(nodeHeap);
  }

  private static long heapBytes(final String size) {
    final Matcher matcher = HEAP_SIZE.matcher(size == null ? "" : size);
    if (!matcher.matches())
      throw new IllegalArgumentException("chaos.nodeHeap must look like 512M or 2G, got " + size);
    final long amount = Long.parseLong(matcher.group(1));
    return Character.toUpperCase(matcher.group(2).charAt(0)) == 'G' ? amount << 30 : amount << 20;
  }

  private static void requireRange(final String name, final Duration min, final Duration max) {
    if (min.isNegative() || max.compareTo(min) < 0)
      throw new IllegalArgumentException(name + "Min/" + name + "Max must satisfy 0 <= min <= max, got " + min + " / " + max);
  }

  private static int integer(final Properties properties, final String name, final int defaultValue) {
    final String value = properties.getProperty(name, "").trim();
    return value.isEmpty() ? defaultValue : Integer.parseInt(value);
  }

  private static Duration duration(final Properties properties, final String name, final String defaultValue) {
    final String value = properties.getProperty(name, "").trim();
    return Duration.parse(value.isEmpty() ? defaultValue : value);
  }
}
