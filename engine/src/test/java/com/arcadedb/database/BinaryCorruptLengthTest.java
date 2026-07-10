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
package com.arcadedb.database;

import com.arcadedb.exception.SerializationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4420: a corrupted/misaligned byte-array length prefix must surface a clear
 * {@link SerializationException} instead of a cryptic {@link NegativeArraySizeException}.
 *
 * <p>A length that decodes to a value larger than {@link Integer#MAX_VALUE} wraps to a small negative int when cast
 * (e.g. {@code 4_294_967_245L} casts to {@code -51}, the exact value reported in the issue), which previously caused
 * {@code new byte[-51]} to blow up with {@code NegativeArraySizeException("-51")}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BinaryCorruptLengthTest {

  @Test
  void lengthWrappingToNegativeIntIsRejectedClearly() {
    // 4_294_967_245L == (2^32 - 51); (int) 4_294_967_245L == -51 (the value reported in issue #4420)
    final long corruptLength = 4_294_967_245L;
    assertThat((int) corruptLength).isEqualTo(-51);

    final Binary buffer = new Binary();
    buffer.putUnsignedNumber(corruptLength);
    buffer.putByteArray(new byte[] { 1, 2, 3 });
    buffer.rewind();

    assertThatThrownBy(buffer::getBytes)
        .isInstanceOf(SerializationException.class)
        .hasMessageContaining("Invalid byte array length")
        .hasMessageContaining(String.valueOf(corruptLength));
  }

  @Test
  void lengthExceedingAvailableBytesIsRejectedClearly() {
    final Binary buffer = new Binary();
    buffer.putUnsignedNumber(5_000); // claims 5000 bytes...
    buffer.putByteArray(new byte[] { 1, 2, 3 }); // ...but only 3 are present
    buffer.rewind();

    assertThatThrownBy(buffer::getBytes)
        .isInstanceOf(SerializationException.class)
        .hasMessageContaining("exceeds");
  }

  @Test
  void validByteArrayStillReadsBackUnchanged() {
    final byte[] payload = "the quick brown fox".getBytes();

    final Binary buffer = new Binary();
    buffer.putBytes(payload);
    buffer.rewind();

    assertThat(buffer.getBytes()).isEqualTo(payload);
  }
}
