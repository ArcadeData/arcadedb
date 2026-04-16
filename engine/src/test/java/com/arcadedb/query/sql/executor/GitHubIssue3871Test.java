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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3871: SQL: DELETE does not resolve variables in type field.
 * DELETE FROM $x should resolve the variable $x to records/RIDs from a prior LET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GitHubIssue3871Test extends TestHelper {

  public GitHubIssue3871Test() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
  }

  /**
   * LET $x = SELECT @rid FROM Doc; DELETE FROM $x
   * should delete the records referenced by $x.
   */
  @Test
  void deleteFromVariableInScript() {
    database.command("sql", "INSERT INTO Doc SET name = 'a'");
    database.command("sql", "INSERT INTO Doc SET name = 'b'");
    assertThat(database.countType("Doc", true)).isEqualTo(2);

    database.command("sqlscript",
        "LET $x = SELECT @rid FROM Doc;\n" +
            "DELETE FROM $x;");

    assertThat(database.countType("Doc", true)).isEqualTo(0);
  }

  /**
   * Verify that DELETE FROM TypeName still works (no regression).
   */
  @Test
  void deleteFromTypeStillWorks() {
    database.command("sql", "INSERT INTO Doc SET name = 'c'");
    assertThat(database.countType("Doc", true)).isEqualTo(1);

    database.command("sql", "DELETE FROM Doc");

    assertThat(database.countType("Doc", true)).isEqualTo(0);
  }
}
