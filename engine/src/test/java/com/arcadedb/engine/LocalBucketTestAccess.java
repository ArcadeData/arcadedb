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

import com.arcadedb.database.DatabaseInternal;

/**
 * Test-scope access to the package-private test seams of {@link LocalBucket} for tests of other packages (Task 14 of #10662), so the
 * seams themselves need not be public.
 */
public final class LocalBucketTestAccess {
  private LocalBucketTestAccess() {
  }

  /** Sets {@link LocalBucket#committedWalkHookForTesting}: run by every committed-chain walk once it read the head chunk; null clears. */
  public static void setCommittedWalkHook(final Runnable hook) {
    LocalBucket.committedWalkHookForTesting = hook;
  }

  /** Opens a publication on the page manager of the database, as an entry half applied would: every committed read meets it open. */
  public static void beginPublication(final DatabaseInternal database) {
    database.getPageManager().beginPublication();
  }

  /** Closes a publication opened by {@link #beginPublication}. */
  public static void endPublication(final DatabaseInternal database) {
    database.getPageManager().endPublication();
  }
}
