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
package com.arcadedb.function.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the configurable RID-to-long encoding used by {@code id()} (Cypher) and {@code .asCypherRID()} (SQL).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IdFunctionEncodingTest {

  @AfterEach
  void restoreDefault() {
    GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.reset();
  }

  @Test
  void defaultIsSixteenBits() {
    assertThat(GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.getValueAsInteger()).isEqualTo(16);
  }

  @Test
  void roundTripsWithDefaultBits() {
    final RID rid = new RID(123, 456);
    final long encoded = IdFunction.encodeRidAsLong(rid);
    assertThat(encoded).isNotNegative();
    assertThat(IdFunction.decodeLongToRidString(encoded)).isEqualTo(rid.toString());
  }

  @Test
  void roundTripsAtBucketAndPositionBoundaries() {
    // 16 bits -> max bucket 65535, 47 bits -> large positions
    final RID rid = new RID(65535, (1L << 47) - 1);
    final long encoded = IdFunction.encodeRidAsLong(rid);
    assertThat(encoded).isNotNegative();
    assertThat(IdFunction.decodeLongToRidString(encoded)).isEqualTo(rid.toString());
  }

  @Test
  void roundTripsWithCustomBits() {
    GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.setValue(20);
    final RID rid = new RID(1_000_000, 9_999_999);
    final long encoded = IdFunction.encodeRidAsLong(rid);
    assertThat(encoded).isNotNegative();
    assertThat(IdFunction.decodeLongToRidString(encoded)).isEqualTo(rid.toString());
  }

  @Test
  void positionThatOverflowedTheOld32BitEncodingNowRoundTrips() {
    // 5 billion > 2^32, would have been truncated by the previous 32/32 split
    final RID rid = new RID(7, 5_000_000_000L);
    final long encoded = IdFunction.encodeRidAsLong(rid);
    assertThat(IdFunction.decodeLongToRidString(encoded)).isEqualTo(rid.toString());
  }

  @Test
  void throwsWhenBucketExceedsReservedBits() {
    // default 16 bits -> max bucket 65535
    final RID rid = new RID(65536, 0);
    assertThatThrownBy(() -> IdFunction.encodeRidAsLong(rid))
        .hasMessageContaining("bucketId")
        .hasMessageContaining("arcadedb.opencypher.idBucketBits");
  }

  @Test
  void throwsWhenPositionExceedsReservedBits() {
    // default 16 bits -> 47 bits for position
    final RID rid = new RID(1, 1L << 47);
    assertThatThrownBy(() -> IdFunction.encodeRidAsLong(rid))
        .hasMessageContaining("position")
        .hasMessageContaining("arcadedb.opencypher.idBucketBits");
  }

  @Test
  void rejectsOutOfRangeConfiguration() {
    assertThatThrownBy(() -> GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.setValue(0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.setValue(32))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
