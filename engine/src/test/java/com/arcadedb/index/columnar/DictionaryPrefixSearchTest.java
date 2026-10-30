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
package com.arcadedb.index.columnar;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DictionaryPrefixSearch} over a dictionary filled through the writer API ({@link GlobalDictionary#getOrAdd}), its rank view
 * rebuilt, then values appended that the view does not cover (the unranked tail, ranked by gap).
 */
class DictionaryPrefixSearchTest {
  /** In insertion order: not sorted, so a search that returned insertion order would fail. */
  private static final String[] RANKED   = { "running", "rum", "b", "run", "a.*b", "rust", "abc", "a.*", "runner", "a.b", "ru", "zeta" };
  /** Appended after the rank view was built: "runa" and "runb" share the gap between "run" and "runner". */
  private static final String[] UNRANKED = { "runb", "runa", "rua" };

  private static GlobalDictionary dictionary(final boolean withTail) {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (final String value : RANKED)
      dictionary.getOrAdd(value);
    // a value of another class sorts after every string: the walk over a prefix stops before it
    dictionary.getOrAdd(42L);
    dictionary.rebuildRankView();
    assertThat(dictionary.rankViewSize()).isEqualTo(RANKED.length + 1);
    if (withTail)
      for (final String value : UNRANKED)
        dictionary.getOrAdd(value);
    return dictionary;
  }

  private static List<Object> search(final GlobalDictionary dictionary, final String prefix, final String after, final int max) {
    final int[] out = new int[Math.max(0, max)];
    final int n = DictionaryPrefixSearch.search(dictionary, prefix, after, max, out);
    final List<Object> values = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      values.add(dictionary.valueOf(out[i]));
    return values;
  }

  @Test
  void prefixRangeInValueOrder() {
    final GlobalDictionary dictionary = dictionary(false);
    assertThat(search(dictionary, "run", null, 10)).containsExactly("run", "runner", "running");
    assertThat(search(dictionary, "ru", null, 10)).containsExactly("ru", "rum", "run", "runner", "running", "rust");
    assertThat(search(dictionary, "run", null, 2)).containsExactly("run", "runner");
    assertThat(search(dictionary, "runz", null, 10)).isEmpty();
    assertThat(search(dictionary, "zz", null, 10)).isEmpty();
    // resumes after a value, in the dictionary or not
    assertThat(search(dictionary, "run", "run", 10)).containsExactly("runner", "running");
    assertThat(search(dictionary, "run", "runn", 10)).containsExactly("runner", "running");
    assertThat(search(dictionary, "run", "running", 10)).isEmpty();
    // a cursor below the prefix range starts at the range
    assertThat(search(dictionary, "run", "a", 10)).containsExactly("run", "runner", "running");
    assertThat(search(dictionary, "run", null, 0)).isEmpty();
  }

  @Test
  void unrankedTailIsMergedInOrder() {
    final GlobalDictionary dictionary = dictionary(true);
    assertThat(dictionary.size()).isGreaterThan(dictionary.rankViewSize());
    assertThat(search(dictionary, "run", null, 10)).containsExactly("run", "runa", "runb", "runner", "running");
    assertThat(search(dictionary, "ru", null, 10)).containsExactly("ru", "rua", "rum", "run", "runa", "runb", "runner", "running", "rust");
    // two unranked values share one gap rank: resuming after the first returns the second, never both or neither
    assertThat(search(dictionary, "run", "runa", 10)).containsExactly("runb", "runner", "running");
    assertThat(search(dictionary, "run", "runa", 1)).containsExactly("runb");
    assertThat(search(dictionary, "run", "runb", 1)).containsExactly("runner");
    assertThat(search(dictionary, "run", null, 3)).containsExactly("run", "runa", "runb");
  }

  @Test
  void withoutRankViewEveryValueIsTheTail() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (final String value : RANKED)
      dictionary.getOrAdd(value);
    assertThat(dictionary.sortedOrdinals()).isEmpty();
    assertThat(search(dictionary, "run", null, 10)).containsExactly("run", "runner", "running");
    assertThat(search(dictionary, "run", "run", 1)).containsExactly("runner");
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "run")).isZero();
  }

  /** A caller that charged the tail for a size it read: the ordinals appended after that size are not scanned. */
  @Test
  void sizeBoundCapsTheUnrankedTail() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (final String value : RANKED)
      dictionary.getOrAdd(value);
    final int bound = dictionary.size();
    dictionary.getOrAdd("runa");
    dictionary.getOrAdd("rua");
    final int[] out = new int[10];
    final int n = DictionaryPrefixSearch.search(dictionary, "ru", null, 10, out, bound);
    final List<Object> values = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      assertThat(out[i]).isLessThan(bound);
      values.add(dictionary.valueOf(out[i]));
    }
    assertThat(values).containsExactly("ru", "rum", "run", "runner", "running", "rust");
    // a bound above the size is the size
    assertThat(DictionaryPrefixSearch.search(dictionary, "ru", null, 10, out, Integer.MAX_VALUE)).isEqualTo(8);
  }

  @Test
  void emptyPrefixReturnsFirstMax() {
    final GlobalDictionary dictionary = dictionary(true);
    assertThat(search(dictionary, "", null, 4)).containsExactly("a.*", "a.*b", "a.b", "abc");
    assertThat(search(dictionary, "", "rum", 3)).containsExactly("run", "runa", "runb");
    // only strings start with a prefix, even an empty one
    assertThat(search(dictionary, "", "zeta", 10)).isEmpty();
  }

  @Test
  void regexCharactersAreLiteral() {
    final GlobalDictionary dictionary = dictionary(true);
    assertThat(search(dictionary, "a.*", null, 10)).containsExactly("a.*", "a.*b");
    assertThat(search(dictionary, "a.", null, 10)).containsExactly("a.*", "a.*b", "a.b");
    assertThat(search(dictionary, "[", null, 10)).isEmpty();
  }

  /**
   * The match is case-sensitive and by UTF-16 code units, with no Unicode normalization: a precomposed "é" and "e" with a combining
   * accent are two prefixes, and a supplementary-plane character (a surrogate pair) is a prefix like any other.
   */
  @Test
  void caseAndNonAsciiAreMatchedByCodeUnits() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final String precomposed = "\u00e9t\u00e9";
    final String combining = "e\u0301t\u00e9";
    final String clef = "\uD834\uDD1E-clef";
    final String clefTwo = "\uD834\uDD1E\uD834\uDD1E";
    for (final String value : new String[] { "Run", "run", "RUN", "running", precomposed, combining, "\u00e9cole", clef, clefTwo, "\uffff" })
      dictionary.getOrAdd(value);
    dictionary.rebuildRankView();
    dictionary.getOrAdd("Runner");
    dictionary.getOrAdd("\u00e9t\u00e9s");
    assertThat(search(dictionary, "run", null, 10)).containsExactly("run", "running");
    assertThat(search(dictionary, "Run", null, 10)).containsExactly("Run", "Runner");
    assertThat(search(dictionary, "RUN", null, 10)).containsExactly("RUN");
    assertThat(search(dictionary, "\u00e9", null, 10)).containsExactly("\u00e9cole", precomposed, "\u00e9t\u00e9s");
    assertThat(search(dictionary, "e\u0301", null, 10)).containsExactly(combining);
    assertThat(search(dictionary, "e", null, 10)).containsExactly(combining);
    // a surrogate pair sorts by its code units (below U+FFFF), as String.compareTo does
    assertThat(search(dictionary, "\uD834\uDD1E", null, 10)).containsExactly(clef, clefTwo);
    assertThat(search(dictionary, "\uD834\uDD1E", clef, 10)).containsExactly(clefTwo);
    assertThat(search(dictionary, "\uD834", null, 10)).containsExactly(clef, clefTwo);
    assertThat(search(dictionary, "\uffff", null, 10)).containsExactly("\uffff");
  }

  @Test
  void rankAfterValueNotInDictionary() {
    final GlobalDictionary dictionary = dictionary(true);
    final int run = dictionary.ordinalOf("run");
    // the gap before "run": every value between "rum" and "run" ranks there
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "rum2")).isEqualTo(dictionary.rank(run) - 1);
    // a view value: the gap after it, where the unranked values just above it rank
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "run")).isEqualTo(dictionary.rank(run) + 1);
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "run")).isEqualTo(dictionary.rank(dictionary.ordinalOf("runa")));
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "runa")).isEqualTo(dictionary.rank(dictionary.ordinalOf("runb")));
    // below every value, and above every value (the number sorts after every string)
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, "")).isZero();
    assertThat(DictionaryPrefixSearch.rankAfter(dictionary, 1_000L)).isEqualTo(2 * dictionary.rankViewSize());
  }

  @Test
  void matchesASortedReferenceOnRandomData() {
    final Random random = new Random(7L);
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final TreeSet<String> reference = new TreeSet<>();
    for (int i = 0; i < 3_000; i++) {
      final String value = randomWord(random);
      reference.add(value);
      dictionary.getOrAdd(value);
      if (i == 2_000)
        dictionary.rebuildRankView();
    }
    assertThat(dictionary.size()).isGreaterThan(dictionary.rankViewSize());
    for (final String prefix : new String[] { "", "a", "ab", "c", "cab", "zz" }) {
      // pages of 7 resumed after the last value equal the whole prefix range
      final List<Object> paged = new ArrayList<>();
      String after = null;
      while (true) {
        final List<Object> page = search(dictionary, prefix, after, 7);
        paged.addAll(page);
        if (page.size() < 7)
          break;
        after = (String) page.getLast();
      }
      final List<String> expected = new ArrayList<>();
      for (final String value : reference)
        if (value.startsWith(prefix))
          expected.add(value);
      assertThat(paged).as(prefix).containsExactlyElementsOf(expected);
    }
  }

  private static String randomWord(final Random random) {
    final int length = 1 + random.nextInt(5);
    final StringBuilder word = new StringBuilder(length);
    for (int i = 0; i < length; i++)
      word.append((char) ('a' + random.nextInt(4)));
    return word.toString();
  }
}
