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

package com.arcadedb.containers.ha.chaos;

import java.io.IOException;

/**
 * Reads what one node holds, from that node's local state (follower reads default to eventual consistency, so a
 * follower answers from what it has applied).
 */
public interface NodeReader {
  /** @return {@code {ChaosOp rows, NEXT edges}} as seen by the node */
  long[] counts(int node) throws IOException;

  /** Streams every ChaosOp row of the node into the snapshot, in key order. */
  void scan(int node, NodeSnapshot sink) throws IOException;
}
