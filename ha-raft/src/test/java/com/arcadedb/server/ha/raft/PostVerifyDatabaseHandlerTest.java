/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PostVerifyDatabaseHandler} database name validation.
 */
class PostVerifyDatabaseHandlerTest {

  @ParameterizedTest
  @ValueSource(strings = { "mydb", "MyDatabase", "test_db", "db-name", "db.v2", "A", "db123", "my-db.test_v2" })
  void validDatabaseNamesMatch(final String name) {
    assertThat(PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher(name).matches()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "../etc/passwd", "db/../secret", "db/../../admin", ".hidden", "-startsWithDash",
      "name with spaces", "db;drop", "db&cmd=1", "db?q=1", "db%00null", "" })
  void invalidDatabaseNamesAreRejected(final String name) {
    assertThat(PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher(name).matches()).isFalse();
  }

  @Test
  void pathTraversalSequencesAreRejected() {
    assertThat(PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher("..").matches()).isFalse();
    assertThat(PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher("../..").matches()).isFalse();
    assertThat(PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher("foo/../bar").matches()).isFalse();
  }
}
