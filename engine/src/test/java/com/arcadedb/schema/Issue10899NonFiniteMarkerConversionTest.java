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

import com.arcadedb.serializer.json.NonFiniteNumbers;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10899: the server writes a non-finite FLOAT/DOUBLE as one of the {@link NonFiniteNumbers} markers, and
 * {@link Type#convertOrKeep} - the conversion the remote client reads every typed value through - must turn the marker
 * back into the number instead of handing it to {@code Float.parseFloat}, which refuses {@code PosInfinity}.
 */
class Issue10899NonFiniteMarkerConversionTest {

  @Test
  void floatMarkersDecodeToFloat() {
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, Float.class)).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, Float.class)).isEqualTo(Float.NEGATIVE_INFINITY);
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NAN, Float.class)).isEqualTo(Float.NaN);
  }

  @Test
  void doubleMarkersDecodeToDouble() {
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, Double.class)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, Double.class)).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NAN, Double.class)).isEqualTo(Double.NaN);
  }

  @Test
  void primitiveTargetsDecodeToo() {
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, float.class)).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.NEGATIVE_INFINITY, double.class)).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void markerForANonFloatingTargetIsNotDecoded() {
    // a STRING column that happens to hold the text stays the text
    assertThat(Type.convertOrKeep(null, NonFiniteNumbers.POSITIVE_INFINITY, String.class)).isEqualTo(NonFiniteNumbers.POSITIVE_INFINITY);
  }

  @Test
  void finiteNumbersAreUnaffected() {
    assertThat(Type.convertOrKeep(null, "1.5", Float.class)).isEqualTo(1.5F);
    assertThat(Type.convertOrKeep(null, 2.5D, Float.class)).isEqualTo(2.5F);
    assertThat(Type.convertOrKeep(null, "2.25", Double.class)).isEqualTo(2.25D);
  }
}
