/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ExecutionResponse.
 */
class ExecutionResponseTest {

  @Test
  void createWithStringResponse() {
    final ExecutionResponse response = new ExecutionResponse(200, "success");

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getResponse()).isEqualTo("success");
    assertThat(response.getBinary()).isNull();
    assertThat(response.isBinary()).isFalse();
  }

  @Test
  void createWithBinaryResponse() {
    final byte[] data = {0x01, 0x02, 0x03, 0x04};
    final ExecutionResponse response = new ExecutionResponse(201, data);

    assertThat(response.getCode()).isEqualTo(201);
    assertThat(response.getResponse()).isNull();
    assertThat(response.getBinary()).isEqualTo(data);
    assertThat(response.isBinary()).isTrue();
  }

  @Test
  void createWithDifferentStatusCodes() {
    final ExecutionResponse ok = new ExecutionResponse(200, "ok");
    final ExecutionResponse created = new ExecutionResponse(201, "created");
    final ExecutionResponse badRequest = new ExecutionResponse(400, "bad request");
    final ExecutionResponse serverError = new ExecutionResponse(500, "error");

    assertThat(ok.getCode()).isEqualTo(200);
    assertThat(created.getCode()).isEqualTo(201);
    assertThat(badRequest.getCode()).isEqualTo(400);
    assertThat(serverError.getCode()).isEqualTo(500);
  }

  @Test
  void createWithEmptyStringResponse() {
    final ExecutionResponse response = new ExecutionResponse(204, "");

    assertThat(response.getCode()).isEqualTo(204);
    assertThat(response.getResponse()).isEmpty();
    assertThat(response.isBinary()).isFalse();
  }

  @Test
  void createWithEmptyBinaryResponse() {
    final ExecutionResponse response = new ExecutionResponse(204, new byte[0]);

    assertThat(response.getCode()).isEqualTo(204);
    assertThat(response.getBinary()).isEmpty();
    assertThat(response.isBinary()).isTrue();
  }

  @Test
  void createWithLargeStringResponse() {
    final String largeResponse = "x".repeat(100000);
    final ExecutionResponse response = new ExecutionResponse(200, largeResponse);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getResponse()).hasSize(100000);
    assertThat(response.isBinary()).isFalse();
  }

  @Test
  void createWithLargeBinaryResponse() {
    final byte[] largeData = new byte[100000];
    final ExecutionResponse response = new ExecutionResponse(200, largeData);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getBinary()).hasSize(100000);
    assertThat(response.isBinary()).isTrue();
  }

  @Test
  void createWithJsonResponse() {
    final String jsonResponse = "{\"status\":\"ok\",\"count\":42}";
    final ExecutionResponse response = new ExecutionResponse(200, jsonResponse);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getResponse()).isEqualTo(jsonResponse);
    assertThat(response.getResponse()).contains("\"status\":\"ok\"");
    assertThat(response.isBinary()).isFalse();
  }

  @Test
  void createWithNullStringResponse() {
    final ExecutionResponse response = new ExecutionResponse(200, (String) null);

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(response.getResponse()).isNull();
    assertThat(response.getBinary()).isNull();
    assertThat(response.isBinary()).isFalse();
  }

  @Test
  void binaryResponsePreservesExactBytes() {
    final byte[] data = {(byte) 0xFF, (byte) 0x00, (byte) 0xAB, (byte) 0xCD};
    final ExecutionResponse response = new ExecutionResponse(200, data);

    assertThat(response.getBinary()).containsExactly((byte) 0xFF, (byte) 0x00, (byte) 0xAB, (byte) 0xCD);
  }
}
