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
package com.arcadedb.database.async;

/**
 * A held quiescence of a database's asynchronous executor: while it is open, every worker of that executor is parked
 * with its transaction batch committed, so nothing the async API was asked to do is either unwritten or in flight.
 * <p>
 * Obtained from {@link com.arcadedb.database.DatabaseInternal#quiesceAsync()} and released by {@link #close()}, which
 * is why it is an {@link AutoCloseable} with no checked exception: it exists to be used in a try-with-resources
 * around a scan-based index build, and a release that could fail would be one more thing between a parked worker and
 * its release.
 * <p>
 * <b>Close it on the thread that opened it.</b> The quiescence is serialized by a reentrant lock held for its whole
 * duration - which is what lets a nested one ride on the outer instead of deadlocking - and a reentrant lock can only
 * be released by its owner. try-with-resources satisfies this by construction; handing the handle to another thread
 * does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface AsyncQuiesce extends AutoCloseable {
  /** Releases every worker parked by this quiescence. Idempotent, and to be called on the opening thread. */
  @Override
  void close();
}
