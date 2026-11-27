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
package com.arcadedb.bench.workload.issues;

import com.arcadedb.bench.runner.Stats;

import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * Times two forms of the same work (or every size of a sweep) in alternating rounds, so drift (JIT, GC, cache) hits them
 * equally. A sample is bounded by time: it runs rounds until it has lasted its target.
 */
public final class IssueSampler {
  private final LongSupplier nanos;

  public IssueSampler(final LongSupplier nanos) {
    this.nanos = nanos;
  }

  /** The medians per execution of each form (ms), and how long the samples took in all (ms). */
  public record Samples(double[] medianMs, double elapsedMs) {
  }

  /** The clock of the sampler, in nanoseconds: the scenarios calibrate on it too. */
  public long nanos() {
    return nanos.getAsLong();
  }

  /**
   * Times the slow and the reference form alternately, the form that starts a sample swapping on every sample. A sample is
   * bounded by time: rounds of {@code batch} executions of each form run until the sample has lasted {@code minSampleMs}, so a
   * batch sized from a cold estimate cannot make it shorter.
   */
  public Samples alternateFor(final Runnable slow, final Runnable reference, final int samples, final int batch,
      final double minSampleMs) {
    return rotateFor(new Runnable[] { slow, reference }, samples, new int[] { batch, batch }, minSampleMs);
  }

  /**
   * Times each form in turn, in rounds of {@code batches[i]} executions of form {@code i}, until a sample has lasted
   * {@code minSampleMs} (at least one round). The rounds of sample {@code s} start with form {@code s % forms.length}, so no
   * form always runs first. Returns the median per execution of each form, and the time all the samples took.
   */
  public Samples rotateFor(final Runnable[] forms, final int samples, final int[] batches, final double minSampleMs) {
    final int n = forms.length;
    final double[][] ms = new double[n][samples];
    final long[] spent = new long[n];
    final long[] executions = new long[n];
    final long minSampleNanos = (long) (minSampleMs * 1_000_000.0);
    final long begin = nanos.getAsLong();
    for (int s = 0; s < samples; s++) {
      Arrays.fill(spent, 0L);
      Arrays.fill(executions, 0L);
      final long sampleStart = nanos.getAsLong();
      do {
        for (int k = 0; k < n; k++) {
          final int f = (s + k) % n;
          final long start = nanos.getAsLong();
          for (int i = 0; i < batches[f]; i++)
            forms[f].run();
          spent[f] += nanos.getAsLong() - start;
          executions[f] += batches[f];
        }
      } while (nanos.getAsLong() - sampleStart < minSampleNanos);
      for (int f = 0; f < n; f++)
        ms[f][s] = spent[f] / 1_000_000.0 / executions[f];
    }
    final double elapsedMs = (nanos.getAsLong() - begin) / 1_000_000.0;
    final double[] medians = new double[n];
    for (int f = 0; f < n; f++)
      medians[f] = Stats.median(ms[f]);
    return new Samples(medians, elapsedMs);
  }

  /** Sizes a batch so that one sample takes about targetMs. */
  public static int batchFor(final double msPerExecution, final double targetMs) {
    if (!(msPerExecution > 0))
      return 1;
    return (int) Math.max(1, Math.round(targetMs / msPerExecution));
  }
}
