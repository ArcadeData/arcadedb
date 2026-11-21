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
package com.arcadedb.utility;

import com.arcadedb.utility.RegexReadFreeWork.Unparseable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12595: the structure of a pattern says how much it can backtrack without reading a character, which is the work the
 * deadline of {@link TimeBoundRegex} cannot see.
 */
class RegexReadFreeWorkTest {

  private static long ways(final String regex) {
    return RegexReadFreeWork.measure(regex, 0).maxWays();
  }

  @Nested
  class WaysOfOneElement {
    @Test
    void anElementThatReadsHasNoReadFreeWay() {
      assertThat(ways("a")).isEqualTo(1);
      assertThat(ways("[a-z]+")).isEqualTo(1);
      assertThat(ways(".*")).isEqualTo(1);
    }

    @Test
    void anAlternationAddsItsOptions() {
      assertThat(ways("(?:\\A|\\A)")).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A|\\z)")).isEqualTo(3);
      assertThat(ways("(?:|)")).isEqualTo(2);
      // an option that reads adds nothing: it cannot succeed without a character
      assertThat(ways("(?:a|)")).isEqualTo(1);
      assertThat(ways("(?:a|b|c)")).isEqualTo(1);
    }

    @Test
    void aSequenceMultipliesItsElements() {
      assertThat(ways("(?:\\A|\\A)(?:\\A|\\A)")).isEqualTo(4);
      assertThat(ways("\\A" + "(?:\\A|\\A)".repeat(10) + "\\z")).isEqualTo(1024);
      assertThat(ways("\\A" + "(?:\\A|\\A)".repeat(11) + "\\z")).isEqualTo(2048);
    }

    @Test
    void anElementThatMustReadEndsTheRun() {
      // the match reads at 'x', and the deadline gets its look there: two runs of 4, not one of 16
      assertThat(ways("(?:\\A|\\A)(?:\\A|\\A)x(?:\\A|\\A)(?:\\A|\\A)")).isEqualTo(4);
    }

    @Test
    void anOptionalElementCanBeSkippedOnlyWhenItIsTheOnlyWay() {
      // taken or skipped, both without a character
      assertThat(ways("\\A?")).isEqualTo(2);
      assertThat(ways("(?:\\A?)(?:\\A?)(?:\\A?)")).isEqualTo(8);
      // taking 'a' needs a character, so only the skip is free
      assertThat(ways("a?a?a?a?")).isEqualTo(1);
      assertThat(ways("a*b*c*")).isEqualTo(1);
    }

    @Test
    void aGroupThatCanMatchNothingInTwoWaysCountsBoth() {
      assertThat(ways("(a*|b*)")).isEqualTo(2);
      assertThat(ways("(\\d*)?")).isEqualTo(2);
    }

    @Test
    void aLoopIsLeftAfterTheFirstTurnThatConsumedNothing() {
      // the model of the JDK the analysis rests on: not 2^40 paths
      assertThat(ways("(?:\\A|\\A){40}")).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A){0,40}")).isEqualTo(3);
      assertThat(ways("(?:\\A|\\A)*")).isEqualTo(3);
      assertThat(ways("(?:\\A|\\A)+")).isEqualTo(2);
      assertThat(ways("(?:(?:\\A)*)*")).isEqualTo(3);
      assertThat(ways("(?:\\A|\\A)??")).isEqualTo(3);
    }

    @Test
    void aRepetitionOfNothingIsOneWay() {
      // the body is still counted once (its own two ways), but twenty of them are not 2^20: nothing outside multiplies it
      assertThat(ways("(?:\\A|\\A){0}")).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A){0}".repeat(20))).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A){0,0}".repeat(20))).isEqualTo(2);
    }

    @Test
    void aPossessiveLoopOrAnAtomicGroupKeepsItsFirstMatch() {
      // each body is two ways, and none of the twenty multiplies the others: the first match is all that is kept
      assertThat(ways("(?>\\A|\\A)")).isEqualTo(2);
      assertThat(ways("(?>\\A|\\A)".repeat(20))).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A)*+".repeat(20))).isEqualTo(2);
      assertThat(ways("(?:\\A|\\A)++".repeat(20))).isEqualTo(2);
    }

    @Test
    void aLookAroundIsOneWayAtMost() {
      assertThat(ways("(?=\\A|\\A)".repeat(20))).isEqualTo(2);
      assertThat(ways("(?<=\\A|\\A)".repeat(20))).isEqualTo(2);
      // a positive look-around whose body reads cannot succeed without a character
      assertThat(ways("(?=a)(?=b)")).isEqualTo(1);
      // a negative one succeeds where its body fails, which at the end of the input is without a read
      assertThat(ways("(?:(?!a)|(?!a))")).isEqualTo(2);
      assertThat(ways("(?:(?!a)|(?!a))".repeat(12))).isEqualTo(4096);
      assertThat(ways("(?:(?<!a)|(?<!a))".repeat(3))).isEqualTo(8);
    }

    @Test
    void aBackReferenceIsOneWayOnlyWhenItsGroupCanMatchNothing() {
      assertThat(ways("(a)\\1*")).isEqualTo(1);
      assertThat(ways("(a)\\1\\1\\1")).isEqualTo(1);
      assertThat(ways("(a*)(?:\\1|\\1)")).isEqualTo(2);
      assertThat(ways("(?<w>a*)(?:\\k<w>|\\k<w>)")).isEqualTo(2);
      // a reference to a group that is still open, or does not exist yet, fails: it cannot succeed without reading either
      assertThat(ways("(a*\\1)(?:\\1|\\1)")).isEqualTo(1);
      assertThat(ways("(?:\\2|\\2)(a*)(b*)")).isEqualTo(1);
    }

    @Test
    void aWordBoundaryAndTheAnchorsAreOneWay() {
      assertThat(ways("(?:\\b|\\B)(?:^|$)")).isEqualTo(4);
      assertThat(ways("(?:\\G|\\Z|\\z|\\A)")).isEqualTo(4);
      assertThat(ways("(?:\\b{g}|\\b{g})")).isEqualTo(2);
    }

    @Test
    void anInlineFlagIsAnEmptyElement() {
      assertThat(ways("(?i)(?s)(?-i)a")).isEqualTo(1);
      assertThat(ways("(?:(?i)|(?m))")).isEqualTo(2);
    }

    @Test
    void aLeadingRepetitionAppliesToAnEmptyElement() {
      // Pattern reads a '{' where an atom should be as an empty one
      assertThat(ways("a{2}{3}")).isEqualTo(1);
    }

    @Test
    void theLargestRunOfThePatternIsTheAnswer() {
      assertThat(ways("(?:\\A|\\A)x(?:\\A|\\A|\\z)")).isEqualTo(3);
      // inside a look-ahead and inside a group too
      assertThat(ways("(?=(?:\\A|\\A)(?:\\A|\\A)(?:\\A|\\A)x)")).isEqualTo(8);
      assertThat(ways("(?:a(?:\\A|\\A)(?:\\A|\\A))")).isEqualTo(4);
    }

    @Test
    void aLiteralPatternHasNothingToBacktrack() {
      assertThat(RegexReadFreeWork.measure("(?:\\A|\\A)".repeat(40), Pattern.LITERAL).maxWays()).isEqualTo(1);
    }

    @Test
    void theCountsStopAtTheSaturationPoint() {
      assertThat(ways("(?:\\A|\\A)".repeat(200))).isEqualTo(RegexReadFreeWork.SATURATED);
    }
  }

  @Nested
  class ReadingTheSyntax {
    @Test
    void countsTheCapturingGroupsAsPatternDoes() {
      assertThat(RegexReadFreeWork.measure("(a)(?:b)(?<n>c)(?=d)(?<!e)(?>f)(?i)(?x:g)(h)", 0).groups()).isEqualTo(3);
    }

    @Test
    void aQuotationHidesWhatIsInsideIt() {
      final RegexReadFreeWork.Measure measure = RegexReadFreeWork.measure("\\Q(|)[\\E\\A?\\Q*\\E", 0);
      assertThat(measure.groups()).isZero();
      assertThat(measure.maxWays()).isEqualTo(2);
      // an unterminated quotation runs to the end
      assertThat(RegexReadFreeWork.measure("\\A?\\Q(|)", 0).groups()).isZero();
    }

    @Test
    void aQuotationReachesIntoACharacterClass() {
      assertThat(ways("[\\Q]\\E]\\A?")).isEqualTo(2);
    }

    @Test
    void aClassCanHoldWhatWouldOtherwiseBeStructure() {
      assertThat(ways("[(|)]\\A?")).isEqualTo(2);
      assertThat(ways("[]a]\\A?")).isEqualTo(2);
      assertThat(ways("[^]a]\\A?")).isEqualTo(2);
      assertThat(ways("[a[b]]\\A?")).isEqualTo(2);
      assertThat(ways("[a&&[^b]]\\A?")).isEqualTo(2);
      assertThat(ways("[\\]\\[]\\A?")).isEqualTo(2);
      assertThat(ways("[\\p{L}\\x{41}\\N{LATIN SMALL LETTER A}]\\A?")).isEqualTo(2);
    }

    @Test
    void anEscapedMetacharacterIsALiteral() {
      assertThat(ways("\\(\\)\\|\\[\\{\\*\\A?")).isEqualTo(2);
      assertThat(ways("\\x{1F600}\\p{IsAlphabetic}\\N{LATIN SMALL LETTER A}\\cA\\c(\\u0041\\0101\\R\\X\\A?")).isEqualTo(2);
    }

    @Test
    void commentsModeIgnoresWhitespaceAndComments() {
      assertThat(RegexReadFreeWork.measure("(?: \\A | \\A ) # (an unbalanced paren and a | bar\n \\A ?", Pattern.COMMENTS).maxWays())
          .isEqualTo(4);
      // the same pattern without the flag reads the spaces and the '#' as literals, which have to read: no run is left
      assertThat(RegexReadFreeWork.measure("(?: \\A | \\A ) # a\n \\A ?", 0).maxWays()).isEqualTo(1);
    }

    @Test
    void anInlineCommentsFlagIsScopedToItsGroup() {
      // (?x) holds to the end of its group, the following option included, and not past it
      assertThat(ways("(?:(?x) \\A | \\A ) \\A ?")).isEqualTo(2);
      assertThat(ways("(?:(?x)\\A|\\A)\\A?")).isEqualTo(4);
      assertThat(ways("(?x: \\A | \\A )\\A?")).isEqualTo(4);
      // past the group the spaces are literals again, and a literal has to read
      assertThat(ways("(?x: \\A | \\A ) \\A?")).isEqualTo(2);
      // and (?-x:...) turns it off inside a pattern that has it on
      assertThat(ways("(?x)(?-x:\\A|\\A) \\A ?")).isEqualTo(4);
      assertThat(ways("(?x)(?-x: \\A | \\A )")).isEqualTo(1);
    }

    @Test
    void commentsModeIgnoresWhitespaceInsideAClassToo() {
      assertThat(RegexReadFreeWork.measure("[ a # ]\n b ] \\A ?", Pattern.COMMENTS).maxWays()).isEqualTo(2);
    }

    @Test
    void aSyntaxItDoesNotFollowIsNotTrusted() {
      assertThatThrownBy(() -> RegexReadFreeWork.measure("(a", 0)).isInstanceOf(Unparseable.class);
      assertThatThrownBy(() -> RegexReadFreeWork.measure("a)", 0)).isInstanceOf(Unparseable.class);
      assertThatThrownBy(() -> RegexReadFreeWork.measure("[a", 0)).isInstanceOf(Unparseable.class);
      assertThatThrownBy(() -> RegexReadFreeWork.measure("a\\", 0)).isInstanceOf(Unparseable.class);
      assertThatThrownBy(() -> RegexReadFreeWork.measure("*a", 0)).isInstanceOf(Unparseable.class);
      assertThat(RegexReadFreeWork.maxWays("(a", 0)).isEqualTo(RegexReadFreeWork.SATURATED);
    }

    /**
     * The syntax Pattern accepts is wider than any list of it, so the parse is checked against Pattern itself: of random
     * strings made of its metacharacters, the ones Pattern compiles must be followed to the end, and to the same number of
     * capturing groups.
     */
    @Test
    void followsEverythingPatternCompiles() {
      final String[] tokens = { "(", ")", "(?:", "(?=", "(?!", "(?<=", "(?<!", "(?>", "(?<n1>", "(?<n2>", "|", "*", "+", "?", "{2}", "{1,3}", "{0}",
          "{2,}", "[a-z]", "[^a]", "[]a]", "[^]a]", "[a&&[b]]", "[a[b]]", "[\\]]", "[(]", "[#]", "[ ]", "\\A", "\\z", "\\Z", "\\G", "\\b", "\\B", "^", "$",
          ".", "a", "b", "1", "\\1", "\\2", "\\k<n1>", "\\Q", "\\E", "\\Qa*(\\E", "\\Q[\\E", "#", " ", "\n", "(?x)", "(?i)", "(?-x)", "(?x:", "(?d)",
          "\\(", "\\[", "\\{", "\\p{L}", "\\pL", "\\x{41}", "\\x41", "\\cA", "\\c(", "\\R", "\\X", "\\\\", "*?", "++", "??", "*+", "\\u0041", "\\0", "\\012",
          "\\N{LATIN SMALL LETTER A}", "\\b{g}", "\u0085", " ", "\\h", "\\d" };
      final int[] flagSets = { 0, Pattern.COMMENTS, Pattern.COMMENTS | Pattern.UNIX_LINES, Pattern.MULTILINE | Pattern.DOTALL | Pattern.UNIX_LINES };
      final Random random = new Random(12595);
      int compiled = 0;
      for (int i = 0; i < 400_000; i++) {
        final StringBuilder regex = new StringBuilder();
        for (int n = 1 + random.nextInt(12); n > 0; n--)
          regex.append(tokens[random.nextInt(tokens.length)]);
        final int flags = flagSets[random.nextInt(flagSets.length)];

        final Pattern pattern;
        try {
          pattern = Pattern.compile(regex.toString(), flags);
        } catch (final PatternSyntaxException | StackOverflowError e) {
          continue;
        }
        ++compiled;
        final RegexReadFreeWork.Measure measure;
        try {
          measure = RegexReadFreeWork.measure(regex.toString(), flags);
        } catch (final Unparseable e) {
          throw new AssertionError("could not follow '" + regex.toString().replace("\n", "\\n") + "' (flags " + flags + "), which Pattern compiles: " + e.getMessage(), e);
        }
        assertThat(measure.groups()).as("capturing groups of '%s' (flags %d)", regex.toString().replace("\n", "\\n"), flags)
            .isEqualTo(pattern.matcher("").groupCount());
      }
      // the scan reaches the patterns it is about: a token list that compiled nothing would pass the loop above vacuously
      assertThat(compiled).isGreaterThan(30_000);
    }
  }

  @Nested
  class PatternsPeopleWrite {
    @Test
    void theyStayFarBelowTheLimit() {
      final String[] patterns = { "^[\\w.+-]+@[\\w-]+\\.[\\w.-]+$", "^(\\d{1,3}\\.){3}\\d{1,3}$", "(?i)^(?:https?://)?(?:www\\.)?[a-z0-9.-]+\\.[a-z]{2,}(?:/\\S*)?$",
          "^\\s*(\\w+)\\s*=\\s*(.*?)\\s*$", "(a|b)*c", "^(\\d+)?(\\.\\d+)?$", "^(?:(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)(?:\\.(?!$)|$)){4}$",
          "^\\d{4}-\\d{2}-\\d{2}(?:T\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d+)?)?(?:Z|[+-]\\d{2}:?\\d{2})?)?$", "^(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*)$",
          "(\\s*,?\\s*)*", ".*a.*b.*c.*", "^foo.*bar$", "\\bword\\b", "^(?=.*[A-Z])(?=.*[a-z])(?=.*\\d).{8,}$", "(?<year>\\d{4})-(?<month>\\d{2})", "(\\w)\\1+",
          "^(?:(?:\\+|00)33|0)\\s*[1-9](?:[\\s.-]*\\d{2}){4}$", "[^\\s]+(?:\\s+[^\\s]+)*", "^$", "", "(?:)", "(|a)", "(a|)", "x*" };
      for (final String pattern : patterns)
        assertThat(ways(pattern)).as(pattern).isLessThanOrEqualTo(4);
    }
  }
}
