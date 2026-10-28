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

import java.io.IOException;

/** Test access to the WAL housekeeping of {@link TransactionManager} for the columnar tests of other packages. */
public final class ColumnarTransactionManagerTestAccess {
  private ColumnarTransactionManagerTestAccess() {
  }

  /** One runtime WAL rotation pass: every active WAL file retired, then the inactive ones dropped after a data fsync. */
  public static void rotateAndDropWAL(final TransactionManager transactionManager) throws IOException {
    transactionManager.rotateAndDropWALForTesting();
  }

  /** The two-argument {@code cleanWALFiles(dropFiles, force)} pass (no data fsync), as the close and kill paths run it. */
  public static boolean cleanWALFiles(final TransactionManager transactionManager, final boolean dropFiles, final boolean force) {
    return transactionManager.cleanWALFilesForTesting(dropFiles, force, false);
  }
}
