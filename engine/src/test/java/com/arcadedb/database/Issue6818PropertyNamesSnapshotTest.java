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

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #6818: {@code MutableDocument.getPropertyNames()} used to hand out the record's own
 * {@code map.keySet()}, so a {@code remove()}/{@code clear()} on the returned set structurally modified the record
 * behind the back of {@link MutableDocument#remove(String)} - no {@code dirty} flag, no validation - while the
 * read-only sibling {@code ImmutableDocument.getPropertyNames()} returned a snapshot. Same {@code Document} call,
 * two incompatible contracts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6818PropertyNamesSnapshotTest extends TestHelper {

  @Test
  void mutableDocumentPropertyNamesCannotBeMutated() {
    database.getSchema().createDocumentType("Doc6818");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc6818").set("a", 1).set("b", 2);

      final Set<String> names = doc.getPropertyNames();
      assertThatThrownBy(names::clear).isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> names.remove("a")).isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> names.add("c")).isInstanceOf(UnsupportedOperationException.class);

      assertThat(doc.has("a")).isTrue();
      assertThat(doc.has("b")).isTrue();
    });
  }

  @Test
  void propertyNamesAreASnapshotSoIterateAndPruneWorks() {
    database.getSchema().createDocumentType("Doc6818b");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc6818b").set("keep", 1).set("tmp1", 2).set("tmp2", 3);

      // The natural iterate-and-prune loop: it used to throw ConcurrentModificationException.
      for (final String p : doc.getPropertyNames())
        if (p.startsWith("tmp"))
          doc.remove(p);

      assertThat(doc.getPropertyNames()).containsExactly("keep");
    });
  }

  @Test
  void snapshotDoesNotSeeLaterChanges() {
    database.getSchema().createDocumentType("Doc6818c");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc6818c").set("a", 1);

      final Set<String> names = doc.getPropertyNames();
      doc.set("b", 2);

      assertThat(names).containsExactly("a");
      assertThat(doc.getPropertyNames()).containsExactly("a", "b");
    });
  }

  @Test
  void propertyNamesKeepInsertionOrder() {
    database.getSchema().createDocumentType("Doc6818d");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc6818d").set("z", 1).set("m", 2).set("a", 3);
      assertThat(doc.getPropertyNames()).containsExactly("z", "m", "a");
    });
  }

  @Test
  void detachedDocumentPropertyNamesCannotBeMutatedEither() {
    database.getSchema().createDocumentType("Doc6818e");

    database.transaction(() -> database.newDocument("Doc6818e").set("a", 1).set("b", 2).save());

    final Document detached = database.query("sql", "select from Doc6818e").next().getElement().get().detach();

    final Set<String> names = detached.getPropertyNames();
    assertThat(names).containsExactly("a", "b");
    assertThatThrownBy(names::clear).isInstanceOf(UnsupportedOperationException.class);
    assertThat(detached.has("a")).isTrue();
  }
}
