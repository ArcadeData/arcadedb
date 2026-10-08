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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9430: a database nothing replicates sizes its work against its own configuration. The replicated override
 * (the Raft wrapper answering the server's configuration) is covered in {@code ha-raft} by the
 * {@code Issue9430ServerOnly*HATest} classes.
 */
class Issue9430ReplicationConfigurationDefaultTest extends TestHelper {

  @Test
  void aStandaloneDatabaseAnswersItsOwnConfiguration() {
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.isReplicated()).isFalse();
    assertThat(db.getReplicationConfiguration()).isSameAs(db.getConfiguration());
    assertThat(db.getWrappedDatabaseInstance().getReplicationConfiguration()).isSameAs(db.getConfiguration());
  }
}
