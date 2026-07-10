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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4730: UPDATE ... SET map = map.remove(key) did not persist the key removal because the
 * {@code remove()} method mutated the existing map in place, so UPDATE saw the new and current values as the same object
 * and skipped the write.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UpdateRemoveMapKeyTest extends TestHelper {
  public UpdateRemoveMapKeyTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE IssueRemoveMapRepro IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY IssueRemoveMapRepro.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY IssueRemoveMapRepro.acl IF NOT EXISTS MAP");
    });
  }

  @Test
  void setMapRemoveKeyPersists() {
    database.command("sql", "INSERT INTO IssueRemoveMapRepro SET id = 'set_remove', acl = {'u1':'rw','u2':'r'}");

    final ResultSet rs = database.command("sql",
        "UPDATE IssueRemoveMapRepro SET acl = acl.remove('u1') WHERE id = 'set_remove'");
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);

    assertMapHasOnly("set_remove", "u2");
  }

  @Test
  void setMapRemoveAllKeyPersists() {
    database.command("sql", "INSERT INTO IssueRemoveMapRepro SET id = 'set_removeall', acl = {'u1':'rw','u2':'r'}");

    database.command("sql", "UPDATE IssueRemoveMapRepro SET acl = acl.removeAll('u1') WHERE id = 'set_removeall'");

    assertMapHasOnly("set_removeall", "u2");
  }

  @Test
  void setListRemovePersists() {
    database.command("sql", "CREATE PROPERTY IssueRemoveMapRepro.tags IF NOT EXISTS LIST");
    database.command("sql", "INSERT INTO IssueRemoveMapRepro SET id = 'set_list', tags = ['a','b','c']");

    database.command("sql", "UPDATE IssueRemoveMapRepro SET tags = tags.remove('b') WHERE id = 'set_list'");

    // re-read in a fresh transaction to confirm it was actually persisted, not just kept in memory
    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT tags FROM IssueRemoveMapRepro WHERE id = 'set_list'");
    final List<Object> tags = rs.next().getProperty("tags");
    assertThat(tags).containsExactly("a", "c");
  }

  @Test
  void removeEqualsSyntaxPersists() {
    database.command("sql", "INSERT INTO IssueRemoveMapRepro SET id = 'remove_equals', acl = {'u1':'rw','u2':'r'}");

    database.command("sql", "UPDATE IssueRemoveMapRepro REMOVE acl = 'u1' WHERE id = 'remove_equals'");

    assertMapHasOnly("remove_equals", "u2");
  }

  @Test
  void removeSquareBracketSyntaxPersists() {
    database.command("sql", "INSERT INTO IssueRemoveMapRepro SET id = 'remove_square', acl = {'u1':'rw','u2':'r'}");

    database.command("sql", "UPDATE IssueRemoveMapRepro REMOVE acl['u1'] WHERE id = 'remove_square'");

    assertMapHasOnly("remove_square", "u2");
  }

  private void assertMapHasOnly(final String id, final String... expectedKeys) {
    // re-read in a fresh transaction to confirm it was actually persisted, not just kept in memory
    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT acl FROM IssueRemoveMapRepro WHERE id = ?", id);
    final Map<String, Object> acl = rs.next().getProperty("acl");
    assertThat(acl).containsOnlyKeys(expectedKeys);
  }
}
