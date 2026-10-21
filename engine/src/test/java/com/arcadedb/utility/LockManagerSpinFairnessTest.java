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
package com.arcadedb.utility;

import java.util.concurrent.TimeUnit;

/**
 * The FIFO, exclusion and no-lost-lock guarantees of {@link LockManagerFairnessTest} with the spin-then-park hand-off on
 * (issue #10501), at the spin {@code arcadedb.commitLockSpin} suggests: the head waiter spins, a release wakes the next
 * head early, and none of that may reorder, starve or leak.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LockManagerSpinFairnessTest extends LockManagerFairnessTest {
  @Override
  protected LockManager<String, String> newLockManager() {
    return new LockManager<>(TimeUnit.MICROSECONDS.toNanos(50));
  }
}
