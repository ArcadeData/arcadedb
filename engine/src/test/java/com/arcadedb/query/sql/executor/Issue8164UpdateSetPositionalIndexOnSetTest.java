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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8164: {@code ArraySelector.setValue(Set, int, ...)} rebuilt the set into a local
 * {@code result} and wrote it back with {@code target.clear(); target.addAll(result);} from INSIDE the rebuild loop,
 * while the iterator over {@code target} was still live. The second {@code next()} then died with a
 * {@code java.util.ConcurrentModificationException}, so {@code UPDATE ... SET prop[n] = value} ALWAYS failed on a
 * {@code Set}-valued property, leaving the in-flight value truncated to one element.
 * <p>
 * The positional REMOVE on the same value was fixed by #8112 (issue #8032); this is the write half of the same
 * family. The {@code List} branch next door is the control: it has always answered correctly, and is what the same
 * statement gets once the value has been through a save and reload.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8164UpdateSetPositionalIndexOnSetTest extends TestHelper {

  private static final String TYPE = "Issue8164Type";

  public Issue8164UpdateSetPositionalIndexOnSetTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType(TYPE);
  }

  @Test
  void setFirstPositionOnSetReplacesOnlyThatElement() {
    assertSetValueAt(0, List.of("Z", "b", "c"));
  }

  @Test
  void setInteriorPositionOnSetReplacesOnlyThatElement() {
    assertSetValueAt(1, List.of("a", "Z", "c"));
  }

  @Test
  void setLastPositionOnSetReplacesOnlyThatElement() {
    assertSetValueAt(2, List.of("a", "b", "Z"));
  }

  /**
   * An index past the end pads with {@code null} up to it, as the {@code List} branch does - except that a
   * {@code Set} refuses the duplicate, so several gaps collapse into ONE {@code null} and the value lands right
   * after it. The issue's expected {@code [a, b, c, null, null, Z]} is not representable in a set at all; this
   * pins what is, so the divergence from the {@code List} branch is a stated property rather than a surprise.
   */
  @Test
  void setPositionPastTheEndPadsWithASingleNullBecauseASetCollapsesTheGaps() {
    assertSetValueAt(5, Arrays.asList("a", "b", "c", null, "Z"));
    // one gap, where the collapse cannot show, IS exactly what the List branch gives
    assertSetValueAt(3, Arrays.asList("a", "b", "c", "Z"));
  }

  /**
   * The {@code List} control, the shape the same property takes after a save and reload. It answered correctly
   * before the fix and must keep doing so.
   */
  @Test
  void theListBranchAnswersTheSameWayItAlwaysHas() {
    for (final int idx : new int[] { 0, 1, 2 }) {
      final MutableDocument doc = database.newDocument(TYPE);
      doc.set("tags", new ArrayList<>(List.of("a", "b", "c")));
      doc.save();

      database.command("sql", "UPDATE " + TYPE + " SET tags[" + idx + "]='Z' WHERE @rid = ?", doc.getIdentity());

      final List<String> expected = new ArrayList<>(List.of("a", "b", "c"));
      expected.set(idx, "Z");
      assertThat(readPersistedTags(doc)).containsExactlyElementsOf(expected);
    }
  }

  /**
   * A positional SET and the positional REMOVE #8112 fixed must agree on the same value: the two halves of the
   * same statement family stopped disagreeing with this fix.
   */
  @Test
  void setAndRemoveAgreeOnTheSameSetValue() {
    final MutableDocument doc = database.newDocument(TYPE);
    doc.set("tags", new LinkedHashSet<>(List.of("a", "b", "c")));
    doc.save();

    database.command("sql", "UPDATE " + TYPE + " SET tags[1]='Z' WHERE @rid = ?", doc.getIdentity());
    database.command("sql", "UPDATE " + TYPE + " REMOVE tags[0] WHERE @rid = ?", doc.getIdentity());

    assertThat(readPersistedTags(doc)).containsExactly("Z", "c");
  }

  private void assertSetValueAt(final int idx, final List<String> expected) {
    final MutableDocument doc = database.newDocument(TYPE);
    final Set<String> tags = new LinkedHashSet<>(List.of("a", "b", "c"));
    doc.set("tags", tags);
    doc.save();

    database.command("sql", "UPDATE " + TYPE + " SET tags[" + idx + "]='Z' WHERE @rid = ?", doc.getIdentity());

    // the very Set instance the document was saved with: the defect left it truncated to a single element
    assertThat(tags).containsExactlyElementsOf(expected);

    assertThat(readPersistedTags(doc)).containsExactlyElementsOf(expected);
  }

  /** Re-reads in a fresh transaction, to confirm what was actually stored rather than only what is in memory. */
  private List<String> readPersistedTags(final MutableDocument doc) {
    database.commit();
    database.begin();
    try (final ResultSet rs = database.query("sql", "SELECT tags FROM " + TYPE + " WHERE @rid = ?", doc.getIdentity())) {
      return rs.next().getProperty("tags");
    }
  }
}
