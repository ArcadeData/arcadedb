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
package com.arcadedb.server.grpc;

import com.arcadedb.exception.CommandSQLParsingException;
import io.grpc.InternalMetadata;
import io.grpc.InternalStatus;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement whose error text is longer than the metadata a gRPC peer receives is refused with a transport failure instead
 * of the error (issue #12567).
 * <p>
 * The description travels as the {@code grpc-message} metadata entry, UTF-8 then percent-encoded, and a gRPC client caps
 * the metadata it receives (8 KB by default in grpc-java, 16 KB hard limit in grpc-core). The bound is on the ENCODED size,
 * measured here with gRPC's own marshaller.
 */
class GrpcErrorDescriptionLengthTest {

  /**
   * The most bytes the description may take on the wire. The mapper has a constant of its own for it; the test states the
   * limit itself so that it fails on a mapper that sends the whole message.
   */
  private static final int MAX_ENCODED_DESCRIPTION = 6 * 1024;

  @Test
  @DisplayName("A description longer than the metadata limit is cut, so the error still reaches the client")
  void aLongDescriptionIsCut() {
    final String description = "SQL syntax error at line 1, column 36: " + "x".repeat(40_000);

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandSQLParsingException(description), null);

    final String sent = sre.getStatus().getDescription();
    assertThat(encodedLength(sent)).isLessThanOrEqualTo(MAX_ENCODED_DESCRIPTION);
    // The verdict still travels: the status code is what the client acts on.
    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    // What the exception said is still there, at the front, where its own words are.
    assertThat(sent).startsWith("SQL syntax error at line 1, column 36: ");
    assertThat(sent).endsWith("... (truncated)");
  }

  @Test
  @DisplayName("A description that fits is sent whole")
  void aShortDescriptionIsSentWhole() {
    final String description = "SQL syntax error at line 1, column 36: mismatched input";

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandSQLParsingException(description), null);

    assertThat(sre.getStatus().getDescription()).isEqualTo(description);
  }

  @Test
  @DisplayName("A short non-ASCII description is sent whole")
  void aShortNonAsciiDescriptionIsSentWhole() {
    final String description = "Type with name '\u8868\u683c\uD83D\uDE00' was not found\n100%";

    assertThat(GrpcErrorMapper.boundDescription(description)).isEqualTo(description);
  }

  @Test
  @DisplayName("CJK text is bounded by its encoded size, not by its characters")
  void cjkIsBoundedByEncodedSize() {
    assertBoundedPrefix("Type with name '" + "\u8868".repeat(10_000) + "' was not found");
  }

  @Test
  @DisplayName("Emoji are bounded by their encoded size and never split between their two surrogates")
  void emojiAreNeverSplit() {
    // A 1-char prefix shifts the surrogate pairs, so whichever parity the cut lands on is exercised
    assertBoundedPrefix("\uD83D\uDE00".repeat(10_000));
    assertBoundedPrefix("x" + "\uD83D\uDE00".repeat(10_000));
  }

  @Test
  @DisplayName("Newlines and percent signs, which are escaped on the wire, count three bytes each")
  void escapedAsciiIsBoundedByEncodedSize() {
    assertBoundedPrefix("\n".repeat(10_000));
    assertBoundedPrefix("%".repeat(10_000));
    assertBoundedPrefix("SQL syntax error\n" + "a%\n~".repeat(5_000));
  }

  /**
   * The bound description fits the encoded budget, is a prefix of the original cut at a whole code point, and the cut is
   * marked.
   */
  private static void assertBoundedPrefix(final String description) {
    final String sent = GrpcErrorMapper.boundDescription(description);

    assertThat(encodedLength(sent)).isLessThanOrEqualTo(MAX_ENCODED_DESCRIPTION);
    assertThat(sent).endsWith("... (truncated)");
    final String kept = sent.substring(0, sent.length() - "... (truncated)".length());
    assertThat(description).startsWith(kept);
    assertThat(Character.isHighSurrogate(kept.charAt(kept.length() - 1))).isFalse();
    // The cut keeps as much as the budget allows, not a token amount
    assertThat(encodedLength(sent)).isGreaterThan(MAX_ENCODED_DESCRIPTION - 16);
  }

  /**
   * The bytes the description takes as the {@code grpc-message} entry, encoded by gRPC itself.
   */
  private static int encodedLength(final String description) {
    final Metadata metadata = new Metadata();
    metadata.put(InternalStatus.MESSAGE_KEY, description);
    final byte[][] serialized = InternalMetadata.serialize(metadata);
    return serialized[1].length;
  }
}
