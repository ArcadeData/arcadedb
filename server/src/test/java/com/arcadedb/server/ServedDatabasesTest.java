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

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class ServedDatabasesTest {

  @Test
  void anOpenDatabaseIsRealAndNamedAsAsked() {
    final ServedDatabases served = new ServedDatabases();
    try {
      final ServerDatabase database = served.open("orders");

      assertThat(database.getName()).isEqualTo("orders");
      assertThat(database.isOpen()).isTrue();
      assertThat(database.getSchema().getTypes()).isEmpty();
      assertThat(database.getEmbedded()).isSameAs(database.getWrappedDatabaseInstance().getEmbedded());
    } finally {
      served.afterEach(null);
    }
  }

  @Test
  void aClosedDatabaseIsAHandleOnAClosedDatabase() {
    final ServedDatabases served = new ServedDatabases();
    try {
      assertThat(served.closed("gone").isOpen()).isFalse();
    } finally {
      served.afterEach(null);
    }
  }

  @Test
  void everyDatabaseIsClosedAndDeletedAfterTheTest() {
    final ServedDatabases served = new ServedDatabases();
    final ServerDatabase first = served.open("same-name");
    final ServerDatabase second = served.open("same-name");
    final File firstDirectory = new File(first.getDatabasePath());
    final File secondDirectory = new File(second.getDatabasePath());
    assertThat(firstDirectory).as("two databases of one name do not share a directory").isNotEqualTo(secondDirectory);
    assertThat(served.pending()).isEqualTo(2);

    served.afterEach(null);

    assertThat(first.isOpen()).isFalse();
    assertThat(second.isOpen()).isFalse();
    assertThat(firstDirectory).doesNotExist();
    assertThat(secondDirectory).doesNotExist();
    assertThat(served.pending()).isZero();
  }
}
