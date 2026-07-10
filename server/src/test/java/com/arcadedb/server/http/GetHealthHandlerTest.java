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
package com.arcadedb.server.http;

import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.http.handler.GetHealthHandler;
import com.arcadedb.server.security.ServerSecurityUser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class GetHealthHandlerTest {

  @Test
  void alwaysReturns204() throws Exception {
    // Liveness only: if the handler runs at all, the HTTP layer is up. Server status is irrelevant.
    final GetHealthHandler handler = new GetHealthHandler(mock(HttpServer.class));

    final ExecutionResponse response = handler.execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(204);
  }

  @Test
  void doesNotRequireAuthentication() {
    final GetHealthHandler handler = new GetHealthHandler(mock(HttpServer.class));

    assertThat(handler.isRequireAuthentication()).isFalse();
  }
}
