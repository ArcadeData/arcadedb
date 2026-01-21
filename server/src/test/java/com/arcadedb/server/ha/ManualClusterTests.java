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
package com.arcadedb.server.ha;

import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;

import java.util.concurrent.TimeUnit;

@Tag("ha")
public class ManualClusterTests extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  public static void main(String[] args) throws Exception {
    ManualClusterTests test = new ManualClusterTests();
    test.beginTest();

    // Keep cluster running for manual testing/observation
    System.out.println("Cluster running. Press Ctrl+C to stop.");
    Awaitility.await("manual test running")
        .pollDelay(1000, TimeUnit.SECONDS)
        .atMost(1001, TimeUnit.SECONDS)
        .until(() -> true);

    test.endTest();
  }
}
