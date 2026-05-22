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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression: a 401/403 from the upstream AI gateway (subscription token invalid/expired/disabled)
 * must NEVER be sent as 401/403 to the Studio client. Studio's global ajaxError handler treats any
 * 401 from a local API call as "session expired" and kicks the user out. The exception always maps
 * the upstream status to 502 Bad Gateway while preserving the body code for the client to display.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AiTokenExceptionTest {

  @Test
  void mapsUpstream401To502() {
    final AiTokenException e = new AiTokenException(401, "{\"code\":\"token_invalid\"}");
    assertThat(e.getHttpStatus()).isEqualTo(502);
    assertThat(e.getUpstreamStatus()).isEqualTo(401);
    assertThat(e.getJsonResponse()).contains("token_invalid");
  }

  @Test
  void mapsUpstream403To502() {
    final AiTokenException e = new AiTokenException(403, "{\"code\":\"token_disabled\"}");
    assertThat(e.getHttpStatus()).isEqualTo(502);
    assertThat(e.getUpstreamStatus()).isEqualTo(403);
  }

  @Test
  void messageContainsUpstreamStatusForLogging() {
    final AiTokenException e = new AiTokenException(401, "{}");
    assertThat(e.getMessage()).contains("401");
  }
}
