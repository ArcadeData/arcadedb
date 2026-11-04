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
package com.arcadedb.schema;

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10488: {@code Type.unwrapNestedPrimitiveArrays} runs on every property write, so a list that holds nothing to replace
 * has to cost a walk and no garbage. A vector held as a list of 1536 floats is the case that matters; measured at about half a
 * microsecond per call. The bound is a tripwire between that walk and one that copies or allocates per element, not a latency
 * budget.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10488UnwrapCostBenchmarkTest {

  @Test
  void aListOfScalarsIsWalkedAndNotCopied() {
    final List<Object> floats = new ArrayList<>();
    for (int i = 0; i < 1536; i++)
      floats.add((float) i / 7);

    // warm up, then measure
    for (int i = 0; i < 20_000; i++)
      assertThat(Type.unwrapNestedPrimitiveArrays(floats)).isSameAs(floats);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    for (int i = 0; i < 200_000; i++)
      if (Type.unwrapNestedPrimitiveArrays(floats) != floats)
        throw new AssertionError("a list with nothing to replace must come back as it went in");

    // 200,000 walks of 1536 elements: about 100 ms when it is a walk, seconds when it allocates per element
    stopwatch.assertGaveUpWithin(5_000, "a walk over the scalars of a list from one that copies or allocates per element");
  }
}
