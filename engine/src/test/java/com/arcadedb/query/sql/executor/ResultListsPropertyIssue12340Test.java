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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12340: {@link Result#listsProperty(String)} is the column test {@code getPropertyNames().contains(name)} without the
 * name set, so it must give the same answer for every shape of row: content only, element only, content over an element, a
 * removed property, a column a projection excluded, and a subclass that lists its names its own way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ResultListsPropertyIssue12340Test extends TestHelper {

  private static final List<String> NAMES = List.of("a", "b", "name", "n", "missing", "@rid");

  private static void assertAgreesWithTheNameSet(final Result row) {
    final Set<String> names = row.getPropertyNames();
    for (final String name : NAMES)
      assertThat(row.listsProperty(name)).as("%s in %s", name, names).isEqualTo(names.contains(name));
  }

  private MutableDocument document() {
    database.getSchema().getOrCreateDocumentType("Doc");
    return database.newDocument("Doc").set("name", "x").set("b", 2);
  }

  @Test
  void contentOnlyRow() {
    final ResultInternal row = new ResultInternal();
    row.setProperty("a", 1);
    row.setProperty("n", null);
    assertThat(row.listsProperty("a")).isTrue();
    assertThat(row.listsProperty("n")).isTrue();
    assertThat(row.listsProperty("missing")).isFalse();
    assertAgreesWithTheNameSet(row);

    row.removeProperty("a");
    assertThat(row.listsProperty("a")).isFalse();
    assertAgreesWithTheNameSet(row);

    row.setProperty("a", 3);
    assertThat(row.listsProperty("a")).isTrue();
    assertAgreesWithTheNameSet(row);
  }

  @Test
  void elementRows() {
    database.transaction(() -> {
      final MutableDocument doc = document();

      final ResultInternal elementOnly = new ResultInternal(doc);
      assertThat(elementOnly.listsProperty("name")).isTrue();
      assertAgreesWithTheNameSet(elementOnly);

      elementOnly.removeProperty("name");
      assertThat(elementOnly.listsProperty("name")).isFalse();
      assertAgreesWithTheNameSet(elementOnly);

      // content over an element: the element is not listed once there is content, although hasProperty() answers for it
      final ResultInternal projected = new ResultInternal().setElement(doc);
      projected.setProperty("n", doc);
      assertThat(projected.hasProperty("b")).isTrue();
      assertThat(projected.listsProperty("b")).isFalse();
      assertAgreesWithTheNameSet(projected);

      final ResultInternal excluded = new ResultInternal(doc);
      excluded.setProjectionExcludes(Set.of("b"));
      assertThat(excluded.listsProperty("b")).isFalse();
      assertAgreesWithTheNameSet(excluded);
    });
  }

  @Test
  void subclassesAndWrappersAnswerWhatTheirNameSetSays() {
    database.transaction(() -> {
      final MutableDocument doc = document();
      final UpdatableResult updatable = new UpdatableResult(doc);
      updatable.setProperty("a", 1);
      assertAgreesWithTheNameSet(updatable);
      assertAgreesWithTheNameSet(new EmptyResult());
    });
  }
}
