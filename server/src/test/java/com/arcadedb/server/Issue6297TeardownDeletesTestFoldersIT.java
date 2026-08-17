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
package com.arcadedb.server;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6297: {@code BaseGraphServerTest.endTest()} must actually delete the folders the test wrote to.
 * <p>
 * It used to call {@code GlobalConfiguration.resetAll()} first, and every path the cleanup deletes is resolved from
 * the configuration at call time, so the whole prefix collapsed and the class's own {@code ./target/databasesN}
 * survived teardown - to be removed, if ever, by the next class's {@code beginTest}. The assertion lives in
 * {@code @AfterAll} because that is the only hook JUnit runs after {@code @AfterEach}, which is where the teardown is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6297TeardownDeletesTestFoldersIT extends BaseGraphServerTest {
  private static String databasePath;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void theServerWroteWhereTheTeardownWillLookForIt() {
    databasePath = getDatabasePath(0);
    assertThat(new File(databasePath)).as("the database the fixture created").exists();
  }

  @AfterAll
  static void theTeardownRemovedIt() {
    assertThat(databasePath).as("the test method must have run").isNotNull();
    assertThat(new File(databasePath)).as("endTest() left the test's own database behind (issue #6297)")
        .doesNotExist();
    assertThat(new File("./target/databases0")).as("endTest() left the server's whole folder behind (issue #6297)")
        .doesNotExist();
  }
}
