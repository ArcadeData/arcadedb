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
package com.arcadedb.bench.env;

import java.util.ArrayList;
import java.util.List;

/**
 * P-core and E-core cpusets of a (possibly hybrid) CPU, parsed from {@code lscpu --extended=CPU,CORE,MAXMHZ}.
 */
public record CpuTopology(String performanceCpus, String efficiencyCpus) {

  public static CpuTopology parse(final String lscpuExtendedOutput) {
    int cpuCol = -1;
    int mhzCol = -1;
    final List<Integer> cpus = new ArrayList<>();
    final List<Double> mhz = new ArrayList<>();
    for (final String line : lscpuExtendedOutput.split("\\R")) {
      final String trimmed = line.trim();
      if (trimmed.isEmpty())
        continue;
      final String[] cols = trimmed.split("\\s+");
      if (cpuCol < 0) {
        for (int i = 0; i < cols.length; i++) {
          if (cols[i].equalsIgnoreCase("CPU"))
            cpuCol = i;
          if (cols[i].equalsIgnoreCase("MAXMHZ"))
            mhzCol = i;
        }
        if (cpuCol < 0 || mhzCol < 0)
          throw new IllegalArgumentException("lscpu output lacks the CPU or MAXMHZ column: " + trimmed);
        continue;
      }
      if (cols.length <= Math.max(cpuCol, mhzCol))
        continue;
      final double value;
      try {
        value = Double.parseDouble(cols[mhzCol]);
      } catch (final NumberFormatException e) {
        continue;
      }
      if (Double.isNaN(value) || Double.isInfinite(value))
        continue;
      cpus.add(Integer.parseInt(cols[cpuCol]));
      mhz.add(value);
    }
    if (cpus.isEmpty())
      throw new IllegalArgumentException("lscpu output has no CPU rows");

    final double[] distinct = mhz.stream().mapToDouble(Double::doubleValue).distinct().sorted().toArray();
    final double max = distinct[distinct.length - 1];
    double bestGap = 0;
    double threshold = Double.NaN;
    for (int i = 1; i < distinct.length; i++) {
      final double gap = distinct[i] - distinct[i - 1];
      if (gap > bestGap) {
        bestGap = gap;
        threshold = distinct[i];
      }
    }

    final List<Integer> performance = new ArrayList<>();
    final List<Integer> efficiency = new ArrayList<>();
    if (bestGap >= max * 0.10) {
      for (int i = 0; i < cpus.size(); i++)
        (mhz.get(i) >= threshold ? performance : efficiency).add(cpus.get(i));
    } else
      performance.addAll(cpus);

    final String perf = toCpuset(performance);
    return new CpuTopology(perf, efficiency.isEmpty() ? perf : toCpuset(efficiency));
  }

  private static String toCpuset(final List<Integer> ids) {
    final int[] sorted = ids.stream().mapToInt(Integer::intValue).sorted().toArray();
    final StringBuilder sb = new StringBuilder();
    int start = sorted[0];
    int prev = start;
    for (int i = 1; i <= sorted.length; i++) {
      if (i < sorted.length && sorted[i] == prev + 1) {
        prev = sorted[i];
        continue;
      }
      if (sb.length() > 0)
        sb.append(',');
      sb.append(start);
      if (prev != start)
        sb.append('-').append(prev);
      if (i < sorted.length) {
        start = sorted[i];
        prev = start;
      }
    }
    return sb.toString();
  }
}
