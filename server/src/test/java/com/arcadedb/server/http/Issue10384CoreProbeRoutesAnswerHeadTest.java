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

import com.arcadedb.server.BaseGraphServerTest;
import io.undertow.server.HttpHandler;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10384: the core dispatcher learned "answer HEAD from the GET entry" by hand, route by route (issue #8133), and the
 * plugin dispatcher learned it once, in the registrar. This pins the core half so the pairs can never be dropped: the two
 * probe routes container healthchecks target answer HEAD with the very handler that answers their GET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10384CoreProbeRoutesAnswerHeadTest extends BaseGraphServerTest {

  @Test
  void theProbeRoutesRegisterHeadNextToGetWithTheSameHandler() {
    final Map<RouteRecordingRoutingHandler.RouteDescriptor, HttpHandler> handlers = getServer(0).getHttpServer()
        .getRegisteredRouteHandlers();

    for (final String path : new String[] { "/api/v1/ready", "/api/v1/health" }) {
      final HttpHandler get = handlers.get(new RouteRecordingRoutingHandler.RouteDescriptor("GET", path));
      final HttpHandler head = handlers.get(new RouteRecordingRoutingHandler.RouteDescriptor("HEAD", path));

      assertThat(get).as("GET " + path).isNotNull();
      assertThat(head).as("HEAD " + path + " must stay registered next to GET (issue #8133)").isSameAs(get);
    }
  }
}
