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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.vector.GroupAdmissionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10002: {@link SQLFunctionVectorAbstract.GroupKeyMemo} answers every search of a pass with the key it first read for
 * a RID, and tells the emit when the record it is about to return no longer has that key - including the {@code null}
 * key of a record with no {@code groupBy} value, which the memo stores under a placeholder.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10002GroupKeyMemoTest extends TestHelper {

  @Test
  void theFirstKeyReadWinsAndAChangedRecordIsNotRankedUnderIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newDocument("Doc").set("grp", 1).save().getIdentity();
      rids[1] = database.newDocument("Doc").save().getIdentity(); // no groupBy value: the null group
      rids[2] = database.newDocument("Doc").set("grp", 2).save().getIdentity();
    });

    final SQLFunctionVectorAbstract.GroupKeyMemo memo = new SQLFunctionVectorAbstract.GroupKeyMemo(database, "grp");
    assertThat(memo.apply(rids[0])).isEqualTo(1);
    assertThat(memo.apply(rids[1])).isNull();
    assertThat(memo.rankedUnder(rids[2], 7)).as("a RID no search resolved has nothing to disagree with").isTrue();

    // A commit lands: the grouped record moves to another group, the ungrouped one gets a group
    database.transaction(() -> {
      rids[0].asDocument().modify().set("grp", 3).save();
      rids[1].asDocument().modify().set("grp", 4).save();
    });

    // Every search of the pass still sees the key it ranked the record under
    assertThat(memo.apply(rids[0])).isEqualTo(1);
    assertThat(memo.apply(rids[1])).isNull();
    // The emit reads the record as it is now, and the memo tells it the record changed under the search
    assertThat(memo.rankedUnder(rids[0], 3)).isFalse();
    assertThat(memo.rankedUnder(rids[1], 4)).isFalse();
    assertThat(memo.rankedUnder(rids[0], 1)).isTrue();
    assertThat(memo.rankedUnder(rids[1], null)).isTrue();
  }

  /** A group key read twice is two arrays with the same content: the record did not change, so it is not stale. */
  @Test
  void anArrayGroupKeyReadAgainIsTheSameKey() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.tags ARRAY_OF_INTEGERS");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("tags", new int[] { 1, 2 }).save().getIdentity());

    final SQLFunctionVectorAbstract.GroupKeyMemo memo = new SQLFunctionVectorAbstract.GroupKeyMemo(database, "tags");
    final Object ranked = memo.apply(rid[0]);
    final Object emitted = rid[0].asDocument(true).get("tags");
    assertThat(emitted).isNotSameAs(ranked);
    assertThat(memo.rankedUnder(rid[0], emitted)).isTrue();
    assertThat(memo.rankedUnder(rid[0], new int[] { 1, 3 })).isFalse();
  }

  @Test
  void anUnreadableRecordIsUnresolvedForTheWholePass() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("grp", 1).save().getIdentity());
    database.transaction(() -> rid[0].asDocument().delete());

    final SQLFunctionVectorAbstract.GroupKeyMemo memo = new SQLFunctionVectorAbstract.GroupKeyMemo(database, "grp");
    assertThat(memo.apply(rid[0])).isSameAs(GroupAdmissionState.UNRESOLVED);
    assertThat(memo.apply(rid[0])).isSameAs(GroupAdmissionState.UNRESOLVED);
  }
}
