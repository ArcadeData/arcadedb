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
 * Carries the upstream HTTP status (for logging) and a pre-formatted JSON error response.
 * <p>
 * The status returned to the client is always {@code 502 Bad Gateway}, regardless of whether the
 * upstream gateway returned 401 or 403. The user's session is valid (they would not have reached
 * this handler otherwise), so propagating 401/403 here would falsely signal "session expired" to
 * the Studio's global error handler and kick the user out. The {@code code} field in the JSON
 * body ({@code token_invalid}, {@code token_expired}, {@code token_disabled}) still lets the
 * Studio show the right "subscription issue" message.
 */
public class AiTokenException extends RuntimeException {
  private static final int CLIENT_HTTP_STATUS = 502;

  private final int    upstreamStatus;
  private final String jsonResponse;

  public AiTokenException(final int upstreamStatus, final String jsonResponse) {
    super("AI token error: upstream returned " + upstreamStatus);
    this.upstreamStatus = upstreamStatus;
    this.jsonResponse = jsonResponse;
  }

  /**
   * HTTP status to send back to the client. Always {@code 502}; never the raw upstream 401/403.
   */
  public int getHttpStatus() {
    return CLIENT_HTTP_STATUS;
  }

  /**
   * Original status returned by the AI gateway. Useful for server-side logging only.
   */
  public int getUpstreamStatus() {
    return upstreamStatus;
  }

  public String getJsonResponse() {
    return jsonResponse;
  }
}
