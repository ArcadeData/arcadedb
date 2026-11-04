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
package com.arcadedb.index.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

import java.util.List;
import java.util.function.ToIntFunction;

/**
 * What the persisted sum of document lengths of a BM25 index was measured with, derived from the thing that measures it instead of
 * restated beside it (issue #10606).
 * <p>
 * The sum cannot be compared with the live data without analyzing every document, so the drift check compares what the sum depends
 * on: the analyzer that tokenizes and the rule that turns the tokens into a length. Naming them is not enough. A class name says
 * nothing about an analyzer whose tokenization changed under the same name (a Lucene upgrade), and a version constant is one more
 * thing to remember to bump. So the signature is what they DO: the length the real analyzer and the real rule give to each sample
 * of a fixed corpus, hashed, plus the Lucene version. Changing the analyzer, its configuration, the library under it or the rule
 * changes the signature, with nothing to remember.
 * <p>
 * The corpus is chosen to make the places where analyzers and rules differ show: nulls and empties, stop words, apostrophes,
 * hyphens, numbers, e-mail addresses and URLs, identifiers, accents, scripts without spaces, an emoji, a token longer than the
 * standard tokenizer's limit, exotic spaces and case folding, and the list and array forms of a value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FullTextLengthSignature {
  private static final long FNV_OFFSET = 0xcbf29ce484222325L;
  private static final long FNV_PRIME  = 0x100000001b3L;

  /** Each row is the values handed to the analyzer for one document, the way a writer hands them. */
  private static final Object[][] CORPUS = { //
      { null }, //
      { "" }, //
      { "   \t\n" }, //
      { "!!! ... ??? --- ;;;" }, //
      { "alpha beta gamma" }, //
      { "The quick brown fox jumps over the lazy dog, and a cat is on the mat of an old man" }, //
      { "Don't stop; it's O'Neil's rock'n'roll" }, //
      { "state-of-the-art e-mail well-known x-ray" }, //
      { "3.14 1,000 1e10 v2.0 2026-10-22 12:30:45" }, //
      { "user.name+tag@example.com https://example.org/a_b/c?d=e&f=g#h" }, //
      { "snake_case camelCase HTTPServer parseXML2Json" }, //
      { "Café naïve façade Zürich ÅNGSTRÖM Crème brûlée" }, //
      { "日本語のテキスト 中文文本测试 한국어 텍스트" }, //
      { "Привет мир, Ελληνικά, עברית, العربية" }, //
      { "emoji 😀 test 👍🏽 ok" }, //
      { "a".repeat(300) + " tail" }, //
      { "a b c\tdef\n\rghi​jkl" }, //
      { "ﬁnal ǅ ß İstanbul ΣΑΣ" }, //
      { List.of("first value", "second", "third one here") }, //
      { new Object[] { "p q", "r" } }, //
      { "one", null, "two three" }, //
      { 42, 3.5d, true }, //
  };

  private FullTextLengthSignature() {
  }

  /**
   * The signature of {@code analyzer} measured with the rule every writer and the rescan use.
   */
  static String of(final Analyzer analyzer) {
    return of(analyzer, LSMTreeFullTextIndex::documentLength);
  }

  /**
   * The signature of {@code analyzer} measured with {@code rule}, which turns the analyzed tokens of a document into the length the
   * corpus counters add up.
   */
  static String of(final Analyzer analyzer, final ToIntFunction<List<String>> rule) {
    long hash = FNV_OFFSET;
    for (final Object[] sample : CORPUS) {
      int length;
      try {
        length = rule.applyAsInt(LSMTreeFullTextIndex.analyze(analyzer, sample));
      } catch (final RuntimeException e) {
        // AN ANALYZER THAT REFUSES A SAMPLE IS A BEHAVIOUR TOO: IT HASHES THE SAME EVERY TIME, AND A CHANGE OF IT CHANGES THE SIGNATURE
        length = -1;
      }
      hash = mix(hash, length);
    }
    return "lucene" + Version.LATEST + ':' + Long.toHexString(hash);
  }

  /** FNV-1a over the four bytes of {@code value}, then a separator, so two samples cannot trade lengths unseen. */
  private static long mix(long hash, final int value) {
    for (int shift = 0; shift < 32; shift += 8) {
      hash ^= (value >>> shift) & 0xff;
      hash *= FNV_PRIME;
    }
    hash ^= 0xff;
    return hash * FNV_PRIME;
  }
}
