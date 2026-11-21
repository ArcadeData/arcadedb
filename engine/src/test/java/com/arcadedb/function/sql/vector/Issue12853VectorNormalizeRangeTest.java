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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12853: {@code vector.normalize} narrowed the magnitude to a float before dividing by it. A vector whose magnitude does not
 * fit a float came back all zeros, and one whose reciprocal does not fit came back Infinity and NaN, though the direction of both
 * is well defined.
 */
class Issue12853VectorNormalizeRangeTest extends TestHelper {

  @Test
  void aMagnitudeBeyondTheFloatRangeKeepsTheDirection() {
    // magnitude 4.2e38
    assertThat(normalize(3.0e38f, 3.0e38f)).containsExactly(0.70710677f, 0.70710677f);
    assertThat(normalize(3.0e38f, -3.0e38f)).containsExactly(0.70710677f, -0.70710677f);
    assertThat(normalize(Float.MAX_VALUE, Float.MAX_VALUE)).containsExactly(0.70710677f, 0.70710677f);
  }

  @Test
  void aMagnitudeBelowTheFloatReciprocalRangeKeepsTheDirection() {
    // 1/mag is beyond Float.MAX_VALUE below ~2.9e-39
    assertThat(normalize(Float.MIN_VALUE, 0f)).containsExactly(1f, 0f);
    assertThat(normalize(1.0e-40f, 1.0e-40f)).containsExactly(0.70710677f, 0.70710677f);
    assertThat(normalize(0f, -Float.MIN_VALUE)).containsExactly(0f, -1f);
  }

  @Test
  void ordinaryVectorsAreUnchanged() {
    assertThat(normalize(3f, 4f)).containsExactly(0.6f, 0.8f);
    assertThat(normalize(1.0e-30f, 1.0e-30f)).containsExactly(0.70710677f, 0.70710677f);
    assertThat(normalize(1.0e20f, 1.0e20f)).containsExactly(0.70710677f, 0.70710677f);
    // a zero vector has no direction: it comes back as it is
    assertThat(normalize(0f, 0f)).containsExactly(0f, 0f);
    final float[] result = VectorUtils.normalize(new float[] { 1f, 2f, 3f, 4f, 5f });
    assertThat(VectorUtils.magnitude(result)).isCloseTo(1f, within(1.0e-6f));
  }

  private float[] normalize(final float... vector) {
    try (final ResultSet rs = database.query("sql", "SELECT vector.normalize(?) AS r", (Object) vector)) {
      return rs.next().getProperty("r");
    }
  }
}
