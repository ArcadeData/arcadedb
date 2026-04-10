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
package com.arcadedb.remote;

/**
 * Result of a remote batch import operation via the /batch HTTP endpoint.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteBatchResult {
  private final long verticesCreated;
  private final long edgesCreated;
  private final long elapsedMs;

  RemoteBatchResult(final long verticesCreated, final long edgesCreated, final long elapsedMs) {
    this.verticesCreated = verticesCreated;
    this.edgesCreated = edgesCreated;
    this.elapsedMs = elapsedMs;
  }

  public long getVerticesCreated() {
    return verticesCreated;
  }

  public long getEdgesCreated() {
    return edgesCreated;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  @Override
  public String toString() {
    return "RemoteBatchResult{verticesCreated=" + verticesCreated + ", edgesCreated=" + edgesCreated + ", elapsedMs=" + elapsedMs + "}";
  }
}
