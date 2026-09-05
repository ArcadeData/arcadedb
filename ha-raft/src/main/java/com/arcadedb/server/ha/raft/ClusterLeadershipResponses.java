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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.ExecutionResponse;

/**
 * The HTTP answers the leadership endpoints share, kept in one place so the shape of the body cannot drift
 * between them (issue #7134).
 * <p>
 * It lives here rather than as an arm of {@code AbstractServerHttpHandler}'s central exception mapping because
 * {@link NotTheLeaderRefusalException} is a Raft type in this module, and the server module - where that mapper
 * lives - must not depend on it. The dependency runs the other way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ClusterLeadershipResponses {

  private ClusterLeadershipResponses() {
    // utility class
  }

  /**
   * 409 Conflict: the request reached a node that is not the leader. The body carries the refusal message, which
   * names the current leader when one is known, so the caller can reissue against it rather than retrying here.
   * Deliberately not used for a failure the LEADER hit while transferring - that means something else and keeps
   * the mapping it has.
   */
  static ExecutionResponse notTheLeader(final NotTheLeaderRefusalException e) {
    return new ExecutionResponse(409, new JSONObject().put("error", e.getMessage()).toString());
  }
}
