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

import com.arcadedb.serializer.json.JSONArray;

import java.util.List;

/**
 * Protocol version exchanged between Studio (or any other AI client) and ArcadeDB's
 * AI Assistant endpoints, used to manage breaking changes without forcing a lock-step
 * upgrade between the browser bundle and the server.
 *
 * <p><b>Versions:</b>
 * <ul>
 *   <li>{@code 1} - initial published protocol. Studio POSTs {@code /api/v1/ai/chat}
 *       with {@code {database, message, chatId?, mode, protocolVersion}}; server
 *       responds either with SSE in auto mode (events {@code tool_start},
 *       {@code tool_end}, {@code done}) or a single JSON body in review-first mode
 *       ({@code {response, commands?, chatId}}).</li>
 * </ul>
 *
 * <p>When introducing a breaking change to either direction, add a new version to
 * {@link #SUPPORTED_VERSIONS} and bump {@link #CURRENT_VERSION}, then branch on
 * the requested version inside the handler. Drop a version only after we are sure
 * no in-the-wild Studio bundle still requests it.
 */
public final class AiProtocol {
  public static final int       CURRENT_VERSION  = 1;
  public static final List<Integer> SUPPORTED_VERSIONS = List.of(1);

  private AiProtocol() {
  }

  public static boolean isSupported(final int version) {
    return SUPPORTED_VERSIONS.contains(version);
  }

  public static JSONArray supportedVersionsArray() {
    final JSONArray a = new JSONArray();
    for (final Integer v : SUPPORTED_VERSIONS)
      a.put(v.intValue());
    return a;
  }
}
