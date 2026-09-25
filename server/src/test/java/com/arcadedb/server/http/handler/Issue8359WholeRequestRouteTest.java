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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8359: only a route that runs its body as one command can have that command forwarded whole, with the client's
 * own body. {@code POST /query} shares {@link PostCommandHandler#execute} but runs its statement on the node that serves
 * it, so a write it issues from inside - through a function it calls - is a part of the request, never the whole of it.
 */
class Issue8359WholeRequestRouteTest {

  @Test
  void theCommandRouteCanBeForwardedWhole() {
    assertThat(new PostCommandHandler(null).commandForwardIsWholeRequest()).isTrue();
  }

  @Test
  void theQueryRouteIsNeverForwardedWhole() {
    assertThat(new PostQueryHandler(null).commandForwardIsWholeRequest()).isFalse();
  }
}
