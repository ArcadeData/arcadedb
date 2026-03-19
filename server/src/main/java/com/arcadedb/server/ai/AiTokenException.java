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
package com.arcadedb.server.ai;

/**
 * Thrown when the AI gateway rejects a subscription token (invalid, expired, disabled).
 * Carries the HTTP status and pre-formatted JSON error response for direct forwarding to the client.
 */
public class AiTokenException extends RuntimeException {
  private final int    httpStatus;
  private final String jsonResponse;

  public AiTokenException(final int httpStatus, final String jsonResponse) {
    super("AI token error: " + httpStatus);
    this.httpStatus = httpStatus;
    this.jsonResponse = jsonResponse;
  }

  public int getHttpStatus() {
    return httpStatus;
  }

  public String getJsonResponse() {
    return jsonResponse;
  }
}
