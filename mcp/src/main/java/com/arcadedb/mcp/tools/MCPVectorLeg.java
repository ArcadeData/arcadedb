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
package com.arcadedb.mcp.tools;

import com.arcadedb.query.search.VectorSearchLeg;

/**
 * The vector-argument bounds the MCP tool schemas advertise, read from the engine-side
 * {@link VectorSearchLeg} that enforces them.
 * <p>
 * The leg itself moved to {@code com.arcadedb.query.search} in issue #7306, when the HTTP
 * {@code /api/v1/vector/*} routes and the gRPC vector RPCs became second and third callers of it. This class is
 * what keeps the numbers the MCP JSON-Schema publishes tied to the numbers the search actually enforces, so the
 * schema cannot advertise a window the engine would reject.
 */
public final class MCPVectorLeg {
  public static final int DEFAULT_K     = VectorSearchLeg.DEFAULT_K;
  public static final int MAX_K         = VectorSearchLeg.MAX_K;
  public static final int MAX_EF_SEARCH = VectorSearchLeg.MAX_EF_SEARCH;

  private MCPVectorLeg() {
  }
}
