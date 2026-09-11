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
package com.arcadedb.engine;

/**
 * Page versions that an ordering authority outside the engine has already assigned, but that the local page files do
 * not reflect yet. In a replicated cluster the leader assigns every committed transaction its page versions in log
 * order; an entry accepted into the log but not yet applied on this node leaves the local copy of its pages one or
 * more versions behind. A transaction validated against that stale copy would ship a delta based on a superseded
 * image (issue #6965), so {@link PageManager#checkPageVersion} treats a reserved version as the most recent one.
 * <p>
 * Installed on a {@link com.arcadedb.database.LocalDatabase} by the replication layer; a standalone database has
 * none, and the check costs nothing there.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface PageVersionReservations {
  /**
   * @return the highest version already assigned to the page and not yet visible locally, or {@code -1} when the local
   * copy is current
   */
  int reservedVersion(PageId pageId);
}
