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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A statement whose error text is longer than the metadata a gRPC peer receives is refused with a transport failure instead
 * of the error (issue #12567).
 * <p>
 * The SQL parser quotes the offending line in its error message, so a statement that is one long line carries every
 * character of it into the message. The message travels as the {@code grpc-message} metadata entry, and gRPC caps the
 * metadata a peer receives at 16 KB: a statement longer than about 12 KB was therefore answered with a bare
 * {@code RESOURCE_EXHAUSTED}, and the caller could not tell a malformed statement from a server that ran out of room. The
 * other transports carry the same message in a body with no such cap.
 */
class GrpcErrorDescriptionLengthTest {

  /**
   * The longest error description this transport sends, in characters. The mapper has a
   * constant of its own for it; the test states the limit itself so that it fails on a
   * mapper that sends the whole message.
   */
  private static final int MAX_DESCRIPTION_LENGTH = 8 * 1024;

  @Test
  @DisplayName("A description longer than the metadata limit is cut, so the error still reaches the client")
  void aLongDescriptionIsCut() {
    final String description = "SQL syntax error at line 1, column 36: " + "x".repeat(40_000);

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandSQLParsingException(description), null);

    final String sent = sre.getStatus().getDescription();
    assertThat(sent).hasSizeLessThanOrEqualTo(MAX_DESCRIPTION_LENGTH + "... (truncated)".length());
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
  @DisplayName("The cut description fits what a gRPC peer receives")
  void theCutDescriptionFitsTheMetadataLimit() {
    // The metadata a peer receives is capped at 16 KB; the description travels in one entry of it.
    assertThat(MAX_DESCRIPTION_LENGTH).isLessThan(16 * 1024);
  }
}
