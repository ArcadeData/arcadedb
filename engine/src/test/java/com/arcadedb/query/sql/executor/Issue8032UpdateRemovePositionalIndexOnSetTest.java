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
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8032: {@code ArraySingleValuesSelector.applyRemove()}'s {@code Set} branch declared its
 * running position as {@code final int count = 0} and never incremented it, so {@code values.contains(count)} tested
 * the same constant on every iteration. {@code REMOVE prop[0]} therefore matched every element and emptied the whole
 * set, while {@code REMOVE prop[n]} for any other {@code n} matched nothing and silently removed no element.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8032UpdateRemovePositionalIndexOnSetTest extends TestHelper {

  public Issue8032UpdateRemovePositionalIndexOnSetTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("Issue8032Type");
  }

  @Test
  void removeFirstPositionOnSetRemovesOnlyThatElement() {
    final MutableDocument doc = database.newDocument("Issue8032Type");
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE Issue8032Type REMOVE tags[0] WHERE @rid = ?", doc.getIdentity());

    // the same Set instance the document was saved with must have exactly 2 elements left, not 0 (in-tx exposure)
    assertThat(tags).containsExactly("b", "c");

    // re-read in a fresh transaction to confirm what was actually persisted, not just kept in memory
    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT tags FROM Issue8032Type");
    final List<Object> persisted = rs.next().getProperty("tags");
    assertThat(persisted).containsExactly("b", "c");
  }

  @Test
  void removeSecondPositionOnSetRemovesOnlyThatElement() {
    final MutableDocument doc = database.newDocument("Issue8032Type");
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE Issue8032Type REMOVE tags[1] WHERE @rid = ?", doc.getIdentity());

    assertThat(tags).containsExactly("a", "c");

    database.commit();
    database.begin();
    final ResultSet rs = database.query("sql", "SELECT tags FROM Issue8032Type");
    final List<Object> persisted = rs.next().getProperty("tags");
    assertThat(persisted).containsExactly("a", "c");
  }
}
