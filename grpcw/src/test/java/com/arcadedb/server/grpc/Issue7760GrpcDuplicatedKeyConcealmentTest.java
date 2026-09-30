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

import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7760 on the gRPC transport: production mode leaves out the {@code arcadedb-dup-keys} trailer (the key
 * values are stored data) and keeps the class and index-name trailers, the same decision the HTTP
 * {@code exceptionArgs} takes ({@code Issue7760DuplicatedKeyConcealmentHttpTest}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7760GrpcDuplicatedKeyConcealmentTest {
  private static final String SECRET = "alice@example.com";

  private static DuplicatedKeyException duplicate() {
    return new DuplicatedKeyException("User[email]", "[" + SECRET + "]", new RID(3, 7));
  }

  @Test
  void productionModeOmitsTheKeysTrailerButKeepsTheIndex() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(duplicate(), "ExecuteCommand", null, true);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)).isNull();
    assertThat(sre.getStatus().getDescription()).doesNotContain(SECRET);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(new String(Base64.getDecoder().decode(sre.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)), StandardCharsets.UTF_8))
        .isEqualTo("User[email]");
  }

  @Test
  void developmentModeKeepsTheKeysTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(duplicate(), "ExecuteCommand", null, false);

    assertThat(new String(Base64.getDecoder().decode(sre.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)), StandardCharsets.UTF_8))
        .isEqualTo("[" + SECRET + "]");
  }
}
