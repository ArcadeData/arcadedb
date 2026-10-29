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

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Lucene's {@link StandardAnalyzer} with the English stop words: the analyzer Neo4j calls {@code standard} (issue #10863). A
 * FULL_TEXT index builds its analyzer from a class name with a constructor that takes no arguments, and
 * {@code new StandardAnalyzer()} has no stop words (it is Neo4j's {@code standard-no-stop-words}), so this class is that
 * constructor: the same tokenizer and lower-casing, then {@link EnglishAnalyzer#ENGLISH_STOP_WORDS_SET} removed, and no
 * stemming.
 */
public final class StandardEnglishStopWordsAnalyzer extends StopwordAnalyzerBase {
  public StandardEnglishStopWordsAnalyzer() {
    super(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    final TokenStream lowerCased = new LowerCaseFilter(source);
    return new TokenStreamComponents(source, new StopFilter(lowerCased, stopwords));
  }

  @Override
  protected TokenStream normalize(final String fieldName, final TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
