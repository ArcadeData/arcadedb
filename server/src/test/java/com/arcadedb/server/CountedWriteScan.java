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
package com.arcadedb.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The census behind issues #10084 and #10085: a write path that replies with a count, or with the record it wrote, must read the
 * engine's answer ({@code saveReportingVeto()}, {@code GraphEngine.newEdgeReportingVeto()}), because the {@code void}
 * {@code save()} and {@code newEdge()} throw it away or raise it. A call site test pins one arm; this scan pins every future arm of a
 * source file: it names the line of every bare {@code .save()} and {@code .newEdge(} call outside a comment, except the lines a
 * caller declares as deliberate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CountedWriteScan {
  private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
  private static final Pattern LINE_COMMENT  = Pattern.compile("//[^\\n]*");
  private static final Pattern BARE_WRITE    = Pattern.compile("\\.save\\(\\)|\\.newEdge\\(");

  private CountedWriteScan() {
  }

  /**
   * @param allowed a pattern a line of code matches when its bare write is deliberate, {@code null} for none
   *
   * @return the 1-based line of every bare {@code .save()} / {@code .newEdge(} call in {@code source}, comments and allowed lines
   * left out
   */
  public static List<Integer> offenders(final String source, final Pattern allowed) {
    final String code = blank(LINE_COMMENT, blank(BLOCK_COMMENT, source));
    final String[] lines = code.split("\n", -1);
    final List<Integer> offenders = new ArrayList<>();
    for (int i = 0; i < lines.length; i++)
      if (BARE_WRITE.matcher(lines[i]).find() && (allowed == null || !allowed.matcher(lines[i]).find()))
        offenders.add(i + 1);
    return offenders;
  }

  /** The offenders of the file at the {@code basedir}-relative {@code path}, Maven's module root or the working directory. */
  public static List<Integer> offendersIn(final String path, final Pattern allowed) throws IOException {
    return offenders(Files.readString(Path.of(System.getProperty("basedir", "."), path)), allowed);
  }

  /** Comments replaced by blanks that keep their line breaks, so the line numbers of the code are unchanged. */
  private static String blank(final Pattern comment, final String source) {
    final Matcher matcher = comment.matcher(source);
    final StringBuilder result = new StringBuilder(source.length());
    while (matcher.find())
      matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group().replaceAll("[^\\n]", " ")));
    matcher.appendTail(result);
    return result.toString();
  }
}
