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
package com.arcadedb.integration.exporter;

import java.util.concurrent.atomic.AtomicLong;

public class ExporterContext {
  public final AtomicLong documents      = new AtomicLong();
  public final AtomicLong vertices       = new AtomicLong();
  public final AtomicLong edges          = new AtomicLong();
  /**
   * Records that threw while being serialized and were skipped rather than aborting the whole export
   * (issue #6471). Incremented by the format implementation's per-record catch blocks; {@link Exporter}
   * surfaces a non-zero count as a failing outcome once the export completes.
   */
  public final AtomicLong skippedRecords = new AtomicLong();
  public       long       startedOn;
  public       long       lastLapOn;
  public       long       lastDocuments;
  public       long       lastVertices;
  public       long       lastEdges;
}
