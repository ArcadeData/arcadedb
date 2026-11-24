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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code SEARCH_INDEX} over a small target is scored from the records themselves (issue #11683), without the complete search of the
 * type, unless the open transaction has pending changes to the index. An application sends its ranking statements inside a transaction
 * (a read-only one included): a transaction that changed nothing in the index must not push every statement to the complete search.
 */
class SearchIndexOpenTransactionPathTest extends TestHelper {
  private static final String[] WORDS = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta" };

  private final List<RID> sample = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
      final Random random = new Random(3);
      for (int i = 0; i < 600; i++) {
        final StringBuilder text = new StringBuilder();
        for (int w = 0; w < 1 + random.nextInt(12); w++)
          text.append(WORDS[random.nextInt(WORDS.length)]).append(' ');
        final var document = database.newDocument("Doc").set("content", text.toString().trim());
        document.save();
        if (i % 40 == 0)
          sample.add(document.getIdentity());
      }
    });
  }

  private Map<RID, Float> scores(final String target) {
    final Map<RID, Float> scores = new HashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS s FROM " + target + " WHERE SEARCH_INDEX('Doc[content]', :q) = true", Map.of("q", "alpha beta"))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.getProperty("rid"), row.getProperty("s"));
      }
    }
    return scores;
  }

  private String list() {
    final List<String> rids = new ArrayList<>();
    for (final RID rid : sample)
      rids.add(rid.toString());
    return "[" + String.join(", ", rids) + "]";
  }

  @Test
  void aTransactionThatChangedNothingInTheIndexStillScoresTheTargetFromItsRecords() {
    final Map<RID, Float> complete = scores("Doc");
    complete.keySet().retainAll(sample);
    assertThat(complete).isNotEmpty();

    database.begin();
    try {
      final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
      assertThat(scores(list())).isEqualTo(complete);
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).as("complete searches").isEqualTo(before);
    } finally {
      database.rollback();
    }
  }
}
