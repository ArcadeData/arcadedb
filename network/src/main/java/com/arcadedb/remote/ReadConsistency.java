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
 * Consistency guarantee requested by a RemoteDatabase client when reading.
 *
 * <ul>
 *   <li>{@link #EVENTUAL}: no cross-node ordering guarantees. No read consistency headers sent.</li>
 *   <li>{@link #READ_YOUR_WRITES}: reads observe at least every write previously observed by this client.</li>
 *   <li>{@link #LINEARIZABLE}: reads observe the latest committed state of the cluster.</li>
 * </ul>
 */
public enum ReadConsistency {
  EVENTUAL,
  READ_YOUR_WRITES,
  LINEARIZABLE
}
