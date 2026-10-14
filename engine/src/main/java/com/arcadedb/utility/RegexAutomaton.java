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

import com.arcadedb.utility.RegexOverAlphabet.Verdict;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A {@link java.util.regex} pattern compiled into a nondeterministic finite automaton over a small, fixed alphabet, so that
 * whether two patterns have a non-empty string in common is DECIDED rather than searched for (issue #9990): the automata
 * are walked in product, which visits every pair of states at most once, and the answer is a proof in both directions
 * whatever the length of the strings involved. {@link RegexOverAlphabet} enumerates strings instead, and so can only give
 * up on a type whose texts are too many to enumerate, the wide types first.
 * <p>
 * Only the regular part of the syntax is compiled: characters, character classes (of every form, intersections and
 * {@code \p{..}} included), {@code .}, groups, alternation, the greedy and reluctant quantifiers and the anchors {@code ^},
 * {@code $}, {@code \A}, {@code \z}, {@code \Z}, plus leading inline flags that change what a character matches ({@code i},
 * {@code u}, {@code U}, {@code s}, {@code m}, {@code d}). Everything else - back references, look-arounds, atomic groups,
 * possessive or stacked quantifiers, word boundaries, inline flags past the start - makes {@link #compile} answer null, and
 * the caller falls back to a search. Exactness comes from never re-implementing what a character matches: every atom is
 * compiled by {@link Pattern} itself and asked about each character of the alphabet, so only the STRUCTURE of the pattern is
 * parsed here.
 * <p>
 * Every string is restricted to the alphabet: a character outside it is a character no string the caller cares about holds.
 * The anchors are only compiled when the alphabet holds no line terminator, the one case where {@code $} and {@code \Z} are
 * plain end-of-input anchors whatever the flags.
 * <p>
 * Not thread safe: the epsilon closures are memoized as they are first needed. Compile one automaton per check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RegexAutomaton {
  /** the most states one automaton may have: a counted repetition is expanded, so {@code (x{100}){100}} is 10,000 copies */
  public static final  int    MAX_STATES              = 50_000;
  /** the most pairs of states the product walk may mark, one bit each */
  private static final long   MAX_PAIRS               = 1L << 26;
  /**
   * the most pairs the walk may queue: each costs a mark and four ints, so this keeps a declaration within a few tens of
   * megabytes whatever the automata, well before MAX_PAIRS could be reached
   */
  private static final int    MAX_QUEUED_PAIRS        = 2_000_000;
  /** the pairs a dense bit set is allocated for up front; a larger product marks them in a set that grows as it is walked */
  private static final long   MAX_DENSE_PAIRS         = 1L << 20;
  /** the most steps the product walk may take, so the time it costs is bounded whatever the two automata */
  private static final long   MAX_WORK                = 50_000_000L;
  /** how many steps of the product walk run between two looks at the clock */
  private static final int    DEADLINE_CHECK_INTERVAL = 4096;
  /** the deepest nesting of groups compiled; the parser, the size estimate and the builder all recurse on it */
  private static final int    MAX_DEPTH               = 200;
  /** the most entries the memoized epsilon closures may hold in all */
  private static final long   MAX_CLOSURE_SIZE        = 8_000_000L;
  private static final String LINE_TERMINATORS        = "\n\r\u0085\u2028\u2029";
  private static final int    ALLOWED_FLAGS           =
      Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS | Pattern.DOTALL | Pattern.MULTILINE
          | Pattern.UNIX_LINES;

  /**
   * The outcome of the product walk.
   *
   * @param verdict the answer
   * @param common  a shortest non-empty string both automata accept when the answer is {@link Verdict#SATISFIABLE}, else null
   */
  record Search(Verdict verdict, String common) {
    private static final Search UNSATISFIABLE = new Search(Verdict.UNSATISFIABLE, null);
    private static final Search UNDECIDED     = new Search(Verdict.UNDECIDED, null);
  }

  private static final byte KIND_EPSILON = 0;
  private static final byte KIND_SYMBOLS = 1;
  // an epsilon move allowed only before the first character is read
  private static final byte KIND_START   = 2;
  // an epsilon move after which no character can be read any more
  private static final byte KIND_END     = 3;
  private static final byte KIND_ACCEPT  = 4;

  private final String alphabet;
  private final byte[] kind;
  // the characters a SYMBOLS state reads, as a bit set over the indexes of the alphabet (at most 128 characters)
  private final long[] symbolsLow;
  private final long[] symbolsHigh;
  // the state a SYMBOLS, START or END state moves to, or the first move of an EPSILON state (-1: none)
  private final int[]  next;
  // the second move of an EPSILON state (-1: none)
  private final int[]  next2;
  private final int    start;
  private final int    size;

  // epsilon closures after the first character, computed when first needed
  private final int[][] closureStates;
  private final byte[]  closureAccepts;
  private       long    closureSize;
  // scratch of the closure walk: a state (twice, see closure()) is seen in the walk whose stamp it carries
  private final int[]   seen;
  private       int     stamp;
  private final IntList stack = new IntList();

  private RegexAutomaton(final String alphabet, final Builder builder, final int start) {
    this.alphabet = alphabet;
    this.size = builder.size;
    this.kind = Arrays.copyOf(builder.kind, size);
    this.symbolsLow = Arrays.copyOf(builder.symbolsLow, size);
    this.symbolsHigh = Arrays.copyOf(builder.symbolsHigh, size);
    this.next = Arrays.copyOf(builder.next, size);
    this.next2 = Arrays.copyOf(builder.next2, size);
    this.start = start;
    this.closureStates = new int[size][];
    this.closureAccepts = new byte[size];
    this.seen = new int[size * 2];
  }

  /**
   * Compiles {@code regex} for full matching ({@link java.util.regex.Matcher#matches()}) over {@code characters}.
   *
   * @return the automaton, or null when the pattern uses a construct this class does not compile, is not a valid pattern,
   * the alphabet holds more than 128 characters or the automaton would exceed {@link #MAX_STATES}
   */
  public static RegexAutomaton compile(final String regex, final String characters) {
    // a character listed twice is one symbol
    final String alphabet = characters.chars().distinct()
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    if (alphabet.isEmpty() || alphabet.length() > 128)
      return null;
    // Pattern reads a surrogate pair as one character, which an automaton over single chars cannot follow
    for (int i = 0; i < alphabet.length(); i++)
      if (Character.isSurrogate(alphabet.charAt(i)))
        return null;
    try {
      final Parser parser = new Parser(rewriteQuotes(regex), alphabet);
      final Node root = parser.parse();
      // refused from the size of the tree before a single state is built: '(x{100}){100}' is known to be 10,000 copies
      if (statesOf(root) > MAX_STATES)
        return null;
      final Builder builder = new Builder();
      final int[] fragment = builder.build(root);
      final int accept = builder.newState(KIND_ACCEPT);
      builder.link(fragment[1], accept);
      return new RegexAutomaton(alphabet, builder, fragment[0]);
    } catch (final Unsupported | PatternSyntaxException e) {
      return null;
    } catch (final StackOverflowError e) {
      // MAX_DEPTH keeps the recursion shallow; a thread started with a tiny stack is still answered, not crashed
      return null;
    }
  }

  /** The alphabet the automaton reads. */
  public String getAlphabet() {
    return alphabet;
  }

  /**
   * Whether {@code text} is accepted, the way {@link java.util.regex.Matcher#matches()} would answer for it. A character
   * outside the alphabet is never accepted.
   */
  public boolean matches(final CharSequence text) {
    // a reference implementation the tests hold the automaton to Pattern with, not a fast one: the declaration-time check
    // walks automata in product and never calls it
    final IntList current = new IntList();
    boolean accepts = closure(start, true, current);
    for (int i = 0; i < text.length(); i++) {
      final int symbol = alphabet.indexOf(text.charAt(i));
      if (symbol < 0)
        return false;
      final IntList following = new IntList();
      accepts = false;
      for (int j = 0; j < current.size; j++) {
        final int state = current.items[j];
        if (reads(state, symbol)) {
          final int target = next[state];
          for (final int s : closureStates(target))
            following.addIfAbsent(s);
          accepts |= closureAccepts(target);
        }
      }
      current.copyFrom(following);
      if (current.size == 0 && !accepts)
        return false;
    }
    return accepts;
  }

  /**
   * Whether some NON-EMPTY string is accepted by both automata, which must have been compiled over the same alphabet.
   */
  public static Verdict intersects(final RegexAutomaton a, final RegexAutomaton b) {
    return intersects(a, b, Long.MAX_VALUE);
  }

  /**
   * As {@link #intersects(RegexAutomaton, RegexAutomaton)}, giving up with {@link Verdict#UNDECIDED} once
   * {@code deadlineNanos} ({@link System#nanoTime()}, see {@link TimeBoundRegex#newDeadline}) is past: the work is bounded by
   * the sizes of the two automata, and the deadline bounds the time that work can take on a slow or busy machine.
   */
  public static Verdict intersects(final RegexAutomaton a, final RegexAutomaton b, final long deadlineNanos) {
    return search(a, b, deadlineNanos).verdict();
  }

  /**
   * The verdict of {@link #intersects} with, when it is {@link Verdict#SATISFIABLE}, a shortest string both automata accept:
   * what lets a test check the answer against {@link Pattern} itself.
   */
  static Search search(final RegexAutomaton a, final RegexAutomaton b, final long deadlineNanos) {
    if (!a.alphabet.equals(b.alphabet))
      throw new IllegalArgumentException("The two automata read different alphabets");
    if ((long) a.size * b.size > MAX_PAIRS)
      return Search.UNDECIDED;

    // a pair of states is the one each automaton is in right after reading the same character, marked once queued. Each
    // queued pair keeps the one it was reached from and the character read, to spell the string out at the end
    final VisitedPairs visited = new VisitedPairs((long) a.size * b.size);
    final IntList queueA = new IntList();
    final IntList queueB = new IntList();
    final IntList parent = new IntList();
    final IntList symbol = new IntList();

    final IntList initialA = new IntList();
    final IntList initialB = new IntList();
    a.closure(a.start, true, initialA);
    b.closure(b.start, true, initialB);

    long work = 0;
    int[] statesA = initialA.toArray();
    int[] statesB = initialB.toArray();
    // the queued pair being expanded, -1 for the start
    int expanded = -1;
    while (true) {
      for (final int p : statesA)
        for (final int q : statesB) {
          if (++work > MAX_WORK || (work & (DEADLINE_CHECK_INTERVAL - 1)) == 1 && System.nanoTime() - deadlineNanos >= 0)
            return Search.UNDECIDED;
          final long low = a.symbolsLow[p] & b.symbolsLow[q];
          final long high = a.symbolsHigh[p] & b.symbolsHigh[q];
          if ((low | high) == 0)
            continue;
          final int targetA = a.next[p];
          final int targetB = b.next[q];
          if (!visited.add((long) targetA * b.size + targetB))
            continue;
          if (queueA.size == MAX_QUEUED_PAIRS)
            return Search.UNDECIDED;
          queueA.add(targetA);
          queueB.add(targetB);
          parent.add(expanded);
          symbol.add(low != 0 ? Long.numberOfTrailingZeros(low) : 64 + Long.numberOfTrailingZeros(high));
          if (a.closureAccepts(targetA) && b.closureAccepts(targetB))
            return new Search(Verdict.SATISFIABLE, spell(a.alphabet, parent, symbol, queueA.size - 1));
        }

      if (a.closureSize > MAX_CLOSURE_SIZE || b.closureSize > MAX_CLOSURE_SIZE)
        return Search.UNDECIDED;
      if (++expanded == queueA.size)
        return Search.UNSATISFIABLE;
      statesA = a.closureStates(queueA.items[expanded]);
      statesB = b.closureStates(queueB.items[expanded]);
    }
  }

  private static String spell(final String alphabet, final IntList parent, final IntList symbol, final int last) {
    final StringBuilder text = new StringBuilder();
    for (int i = last; i >= 0; i = parent.items[i])
      text.append(alphabet.charAt(symbol.items[i]));
    return text.reverse().toString();
  }

  private boolean reads(final int state, final int symbol) {
    return symbol < 64 ? (symbolsLow[state] & (1L << symbol)) != 0 : (symbolsHigh[state] & (1L << (symbol - 64))) != 0;
  }

  private int[] closureStates(final int state) {
    computeClosure(state);
    return closureStates[state];
  }

  private boolean closureAccepts(final int state) {
    computeClosure(state);
    return closureAccepts[state] == 2;
  }

  private void computeClosure(final int state) {
    if (closureStates[state] != null)
      return;
    final IntList states = new IntList();
    closureAccepts[state] = closure(state, false, states) ? (byte) 2 : (byte) 1;
    closureStates[state] = states.toArray();
    closureSize += states.size;
  }

  /**
   * Collects in {@code reading} the SYMBOLS states reachable from {@code from} through epsilon moves, and answers whether
   * the accepting state is reachable too.
   *
   * @param atStart whether no character has been read yet, which is when a start anchor can be crossed
   */
  private boolean closure(final int from, final boolean atStart, final IntList reading) {
    // a state is visited once without and once with the end anchor crossed: past it, nothing can be read any more
    if (++stamp == 0) {
      // wrapped around after 2^32 walks: forget every mark, so no state reads as seen in this one
      Arrays.fill(seen, 0);
      stamp = 1;
    }
    stack.size = 0;
    stack.add(from << 1);
    boolean accepts = false;
    while (stack.size > 0) {
      final int entry = stack.items[--stack.size];
      if (seen[entry] == stamp)
        continue;
      seen[entry] = stamp;
      final int state = entry >>> 1;
      final int ended = entry & 1;
      switch (kind[state]) {
      case KIND_ACCEPT -> accepts = true;
      case KIND_SYMBOLS -> {
        if (ended == 0)
          reading.add(state);
      }
      case KIND_START -> {
        if (atStart)
          stack.add(next[state] << 1 | ended);
      }
      case KIND_END -> stack.add(next[state] << 1 | 1);
      default -> {
        if (next[state] >= 0)
          stack.add(next[state] << 1 | ended);
        if (next2[state] >= 0)
          stack.add(next2[state] << 1 | ended);
      }
      }
    }
    return accepts;
  }

  /**
   * Rewrites every {@code \Q...\E} quotation into one {@code \x{..}} escape per quoted character, the way
   * {@link Pattern} itself removes the quoting before parsing: a quantifier after {@code \E} then applies to the last quoted
   * character, and a quotation inside a character class becomes characters of the class.
   */
  static String rewriteQuotes(final String regex) {
    if (!regex.contains("\\Q"))
      return regex;
    final StringBuilder result = new StringBuilder(regex.length() + 16);
    int i = 0;
    while (i < regex.length()) {
      final char c = regex.charAt(i);
      if (c == '\\' && i + 1 < regex.length()) {
        if (regex.charAt(i + 1) == 'Q') {
          i += 2;
          final int end = regex.indexOf("\\E", i);
          final int stop = end < 0 ? regex.length() : end;
          while (i < stop) {
            final int codePoint = regex.codePointAt(i);
            result.append("\\x{").append(Integer.toHexString(codePoint)).append('}');
            i += Character.charCount(codePoint);
          }
          i = end < 0 ? regex.length() : end + 2;
          continue;
        }
        result.append(c).append(regex.charAt(i + 1));
        i += 2;
        continue;
      }
      result.append(c);
      ++i;
    }
    return result.toString();
  }

  private static boolean containsAnchor(final Node node) {
    return switch (node) {
      case Anchor anchor -> true;
      case Symbols symbols -> false;
      case Sequence sequence -> sequence.items().stream().anyMatch(RegexAutomaton::containsAnchor);
      case Alternation alternation -> alternation.options().stream().anyMatch(RegexAutomaton::containsAnchor);
      case Repetition repetition -> containsAnchor(repetition.item());
    };
  }

  /**
   * An upper bound of the states {@link Builder#build} creates for {@code node}, saturating instead of overflowing, so a
   * pattern too large to expand is refused before anything is allocated for it.
   */
  private static long statesOf(final Node node) {
    return switch (node) {
      case Symbols symbols -> 2;
      case Anchor anchor -> 2;
      case Sequence sequence -> {
        long total = 1;
        for (final Node item : sequence.items())
          total = Math.min(total + statesOf(item), Long.MAX_VALUE / 4);
        yield total;
      }
      case Alternation alternation -> {
        long total = 1;
        for (final Node option : alternation.options())
          total = Math.min(total + statesOf(option) + 1, Long.MAX_VALUE / 4);
        yield total;
      }
      case Repetition repetition -> {
        final long copies = repetition.max() < 0 ? repetition.min() + 1L : repetition.max();
        final long item = statesOf(repetition.item()) + 1;
        yield item > (Long.MAX_VALUE / 4) / Math.max(1, copies) ? Long.MAX_VALUE / 4 : copies * item + 3;
      }
    };
  }

  /**
   * The pairs of states the product walk has queued. A dense bit set for a product small enough, which is every text form
   * against an ordinary pattern; an open-addressing set of longs that grows as pairs are marked for a larger one, so a large
   * product costs what the walk visits rather than what it could visit.
   */
  private static final class VisitedPairs {
    private final long[] bits;
    private       long[] keys;
    private       int    size;

    private VisitedPairs(final long pairs) {
      if (pairs <= MAX_DENSE_PAIRS)
        bits = new long[(int) ((pairs + 63) >>> 6)];
      else {
        bits = null;
        keys = new long[1024];
        Arrays.fill(keys, -1L);
      }
    }

    /** @return true when the pair was not marked yet */
    private boolean add(final long pair) {
      if (bits != null) {
        final int word = (int) (pair >>> 6);
        final long bit = 1L << (pair & 63);
        if ((bits[word] & bit) != 0)
          return false;
        bits[word] |= bit;
        return true;
      }
      if (size * 2 >= keys.length)
        grow();
      if (!insert(keys, pair))
        return false;
      ++size;
      return true;
    }

    private boolean insert(final long[] table, final long pair) {
      final int mask = table.length - 1;
      int slot = (int) (mix(pair) & mask);
      while (table[slot] != -1L) {
        if (table[slot] == pair)
          return false;
        slot = (slot + 1) & mask;
      }
      table[slot] = pair;
      return true;
    }

    private void grow() {
      final long[] previous = keys;
      keys = new long[previous.length * 2];
      Arrays.fill(keys, -1L);
      for (final long pair : previous)
        if (pair != -1L)
          insert(keys, pair);
    }

    private static long mix(final long value) {
      final long h = value * 0x9E3779B97F4A7C15L;
      return h ^ (h >>> 32);
    }
  }

  /** A construct this class does not compile: the caller falls back to a search. */
  private static final class Unsupported extends RuntimeException {
    private static final Unsupported INSTANCE = new Unsupported();

    private Unsupported() {
      super(null, null, false, false);
    }
  }

  private sealed interface Node permits Symbols, Sequence, Alternation, Repetition, Anchor {
  }

  private record Symbols(long low, long high) implements Node {
  }

  private record Sequence(List<Node> items) implements Node {
  }

  private record Alternation(List<Node> options) implements Node {
  }

  /** @param max -1 for no upper bound */
  private record Repetition(Node item, int min, int max) implements Node {
  }

  private record Anchor(boolean start) implements Node {
  }

  /** Parses the structure of a pattern; what a character matches is left to {@link Pattern}. */
  private static final class Parser {
    private final String              source;
    private final String              alphabet;
    private final Map<String, Symbols> atoms = new HashMap<>();
    private       int                 position;
    private       int                 flags;
    private       int                 depth;

    private Parser(final String source, final String alphabet) {
      this.source = source;
      this.alphabet = alphabet;
    }

    private Node parse() {
      parseLeadingFlags();
      final Node root = parseAlternation();
      if (position != source.length())
        throw Unsupported.INSTANCE;
      return root;
    }

    /** {@code (?i)} and its siblings at the very start, which change what every atom matches and nothing else. */
    private void parseLeadingFlags() {
      if (!source.startsWith("(?"))
        return;
      int i = 2;
      int parsed = 0;
      while (i < source.length() && source.charAt(i) != ')') {
        parsed |= switch (source.charAt(i)) {
          case 'i' -> Pattern.CASE_INSENSITIVE;
          case 'u' -> Pattern.UNICODE_CASE;
          case 'U' -> Pattern.UNICODE_CHARACTER_CLASS;
          case 's' -> Pattern.DOTALL;
          case 'm' -> Pattern.MULTILINE;
          case 'd' -> Pattern.UNIX_LINES;
          // a group ('?:', '?=', '?<'...), comments mode, a flag turned off: left to the general parse, or unsupported
          default -> -1;
        };
        if (parsed < 0)
          return;
        ++i;
      }
      if (i == 2 || i == source.length() || (parsed & ~ALLOWED_FLAGS) != 0)
        return;
      flags = parsed;
      position = i + 1;
    }

    private Node parseAlternation() {
      final List<Node> options = new ArrayList<>(2);
      options.add(parseSequence());
      while (position < source.length() && source.charAt(position) == '|') {
        ++position;
        options.add(parseSequence());
      }
      return options.size() == 1 ? options.getFirst() : new Alternation(options);
    }

    private Node parseSequence() {
      final List<Node> items = new ArrayList<>();
      while (position < source.length()) {
        final char c = source.charAt(position);
        if (c == '|' || c == ')')
          break;
        items.add(parseQuantified());
      }
      return items.size() == 1 ? items.getFirst() : new Sequence(items);
    }

    private Node parseQuantified() {
      final Node atom = parseAtom();
      if (position >= source.length())
        return atom;
      // an anchor inside a repetition makes a turn that can match nothing, and Pattern ends the loop after such a turn:
      // '(?:^0*){2}' does not match '0' for Pattern, which the regular language does. Not compiled, so the fallback answers
      // what Pattern answers
      if ("*+?{".indexOf(source.charAt(position)) >= 0 && containsAnchor(atom))
        throw Unsupported.INSTANCE;

      final int min;
      final int max;
      switch (source.charAt(position)) {
      case '*' -> {
        min = 0;
        max = -1;
        ++position;
      }
      case '+' -> {
        min = 1;
        max = -1;
        ++position;
      }
      case '?' -> {
        min = 0;
        max = 1;
        ++position;
      }
      case '{' -> {
        final int close = source.indexOf('}', position);
        if (close < 0)
          throw Unsupported.INSTANCE;
        final String bounds = source.substring(position + 1, close);
        final int comma = bounds.indexOf(',');
        try {
          if (comma < 0) {
            min = Integer.parseInt(bounds);
            max = min;
          } else {
            min = Integer.parseInt(bounds.substring(0, comma));
            max = comma == bounds.length() - 1 ? -1 : Integer.parseInt(bounds.substring(comma + 1));
          }
        } catch (final NumberFormatException e) {
          throw Unsupported.INSTANCE;
        }
        if (min < 0 || (max >= 0 && max < min))
          throw Unsupported.INSTANCE;
        position = close + 1;
      }
      default -> {
        return atom;
      }
      }

      if (position < source.length()) {
        final char suffix = source.charAt(position);
        // reluctant: the same strings match in full, only the way there differs
        if (suffix == '?')
          ++position;
        else if (suffix == '+')
          // possessive: gives up strings a greedy quantifier would match ('a*+a' matches nothing)
          throw Unsupported.INSTANCE;
      }
      // a quantifier on a quantifier is not the repetition it reads as ('a{2}{3}' matches neither 6 nor 3 a's)
      if (position < source.length() && "*+?{".indexOf(source.charAt(position)) >= 0)
        throw Unsupported.INSTANCE;
      return new Repetition(atom, min, max);
    }

    private Node parseAtom() {
      final char c = source.charAt(position);
      switch (c) {
      case '(' -> {
        if (++depth > MAX_DEPTH)
          throw Unsupported.INSTANCE;
        ++position;
        if (position < source.length() && source.charAt(position) == '?') {
          if (source.startsWith("?:", position))
            position += 2;
          else if (source.startsWith("?<", position) && position + 2 < source.length() && Character.isLetter(
              source.charAt(position + 2))) {
            // a named group, not a look-behind
            final int close = source.indexOf('>', position);
            if (close < 0)
              throw Unsupported.INSTANCE;
            position = close + 1;
          } else
            // look-arounds, atomic groups, inline flags past the start
            throw Unsupported.INSTANCE;
        }
        final Node inner = parseAlternation();
        if (position >= source.length() || source.charAt(position) != ')')
          throw Unsupported.INSTANCE;
        ++position;
        --depth;
        return inner;
      }
      case '[' -> {
        final int end = classEnd(position);
        final String token = source.substring(position, end);
        position = end;
        return atom(token);
      }
      case '\\' -> {
        return parseEscape();
      }
      case '.' -> {
        ++position;
        return atom(".");
      }
      case '^' -> {
        ++position;
        return anchor(true);
      }
      case '$' -> {
        ++position;
        return anchor(false);
      }
      case '*', '+', '?', '{' -> throw Unsupported.INSTANCE;
      default -> {
        final int codePoint = source.codePointAt(position);
        final int length = Character.charCount(codePoint);
        final String token = Pattern.quote(source.substring(position, position + length));
        position += length;
        return atom(token);
      }
      }
    }

    private Node anchor(final boolean start) {
      for (int i = 0; i < LINE_TERMINATORS.length(); i++)
        if (alphabet.indexOf(LINE_TERMINATORS.charAt(i)) >= 0)
          throw Unsupported.INSTANCE;
      return new Anchor(start);
    }

    private Node parseEscape() {
      if (position + 1 >= source.length())
        throw Unsupported.INSTANCE;
      final char e = source.charAt(position + 1);
      int end = position + 2;
      switch (e) {
      case 'A' -> {
        position = end;
        return anchor(true);
      }
      case 'z', 'Z' -> {
        position = end;
        return anchor(false);
      }
      case '0' -> {
        // \0n, \0nn, \0mnn with m <= 3
        int digits = 0;
        while (digits < 3 && end < source.length() && source.charAt(end) >= '0' && source.charAt(end) <= '7') {
          if (digits == 2 && source.charAt(end - 2) > '3')
            break;
          ++end;
          ++digits;
        }
      }
      case 'x', 'p', 'P', 'N' -> {
        if (end < source.length() && source.charAt(end) == '{') {
          final int close = source.indexOf('}', end);
          if (close < 0)
            throw Unsupported.INSTANCE;
          end = close + 1;
        } else
          end += e == 'x' ? 2 : e == 'N' ? 0 : 1;
      }
      case 'u' -> end += 4;
      case 'c' -> end += 1;
      case 'd', 'D', 'w', 'W', 's', 'S', 'h', 'H', 'v', 'V', 't', 'n', 'r', 'f', 'a', 'e' -> {
        // one character, or a class of them
      }
      default -> {
        // back references (\1, \k<name>), boundaries (\b, \B, \G), \R, \X and any other letter
        if (Character.isLetterOrDigit(e))
          throw Unsupported.INSTANCE;
        // an escaped punctuation character stands for itself
      }
      }
      if (end > source.length())
        throw Unsupported.INSTANCE;
      final String token = source.substring(position, end);
      position = end;
      return atom(token);
    }

    /** The end (exclusive) of the character class opening at {@code start}. */
    private int classEnd(final int start) {
      int i = start + 1;
      if (i < source.length() && source.charAt(i) == '^')
        ++i;
      // a ']' right after the opening stands for itself
      if (i < source.length() && source.charAt(i) == ']')
        ++i;
      int depth = 1;
      while (depth > 0) {
        if (i >= source.length())
          throw Unsupported.INSTANCE;
        final char c = source.charAt(i);
        if (c == '\\') {
          if (i + 1 >= source.length())
            throw Unsupported.INSTANCE;
          final char e = source.charAt(i + 1);
          i += 2;
          if ((e == 'p' || e == 'P' || e == 'x' || e == 'N') && i < source.length() && source.charAt(i) == '{') {
            final int close = source.indexOf('}', i);
            if (close < 0)
              throw Unsupported.INSTANCE;
            i = close + 1;
          }
        } else if (c == '[') {
          ++depth;
          ++i;
          // the same rule inside a nested class is not settled here
          if (i < source.length() && (source.charAt(i) == ']' || source.charAt(i) == '^' && i + 1 < source.length()
              && source.charAt(i + 1) == ']'))
            throw Unsupported.INSTANCE;
        } else if (c == ']') {
          --depth;
          ++i;
        } else
          ++i;
      }
      return i;
    }

    /** The characters of the alphabet {@code token} matches, asked of {@link Pattern} itself. */
    private Symbols atom(final String token) {
      final Symbols cached = atoms.get(token);
      if (cached != null)
        return cached;
      final Pattern pattern = Pattern.compile(token, flags);
      long low = 0;
      long high = 0;
      for (int i = 0; i < alphabet.length(); i++)
        if (pattern.matcher(String.valueOf(alphabet.charAt(i))).matches()) {
          if (i < 64)
            low |= 1L << i;
          else
            high |= 1L << (i - 64);
        }
      final Symbols symbols = new Symbols(low, high);
      atoms.put(token, symbols);
      return symbols;
    }
  }

  /** Thompson's construction, with primitive arrays. */
  private static final class Builder {
    private byte[] kind        = new byte[64];
    private long[] symbolsLow  = new long[64];
    private long[] symbolsHigh = new long[64];
    private int[]  next        = new int[64];
    private int[]  next2       = new int[64];
    private int    size;

    private int newState(final byte stateKind) {
      if (size == MAX_STATES)
        throw Unsupported.INSTANCE;
      if (size == kind.length) {
        final int capacity = Math.min(kind.length * 2, MAX_STATES);
        kind = Arrays.copyOf(kind, capacity);
        symbolsLow = Arrays.copyOf(symbolsLow, capacity);
        symbolsHigh = Arrays.copyOf(symbolsHigh, capacity);
        next = Arrays.copyOf(next, capacity);
        next2 = Arrays.copyOf(next2, capacity);
      }
      kind[size] = stateKind;
      next[size] = -1;
      next2[size] = -1;
      return size++;
    }

    /** Adds an epsilon move from {@code from}, an EPSILON state with a free move, to {@code to}. */
    private void link(final int from, final int to) {
      if (next[from] < 0)
        next[from] = to;
      else if (next2[from] < 0)
        next2[from] = to;
      else
        throw new IllegalStateException("State " + from + " has no free move");
    }

    /** @return the entry state and the exit state, an EPSILON state with no move yet */
    private int[] build(final Node node) {
      return switch (node) {
        case Symbols symbols -> {
          final int entry = newState(KIND_SYMBOLS);
          symbolsLow[entry] = symbols.low();
          symbolsHigh[entry] = symbols.high();
          final int exit = newState(KIND_EPSILON);
          next[entry] = exit;
          yield new int[] { entry, exit };
        }
        case Anchor anchor -> {
          final int entry = newState(anchor.start() ? KIND_START : KIND_END);
          final int exit = newState(KIND_EPSILON);
          next[entry] = exit;
          yield new int[] { entry, exit };
        }
        case Sequence sequence -> {
          if (sequence.items().isEmpty()) {
            final int state = newState(KIND_EPSILON);
            yield new int[] { state, state };
          }
          int[] result = null;
          for (final Node item : sequence.items()) {
            final int[] fragment = build(item);
            if (result == null)
              result = fragment;
            else {
              link(result[1], fragment[0]);
              result[1] = fragment[1];
            }
          }
          yield result;
        }
        case Alternation alternation -> {
          final int exit = newState(KIND_EPSILON);
          // a chain of binary branches, an EPSILON state having two moves at most
          int entry = -1;
          int branch = -1;
          for (final Node option : alternation.options()) {
            final int[] fragment = build(option);
            link(fragment[1], exit);
            final int split = newState(KIND_EPSILON);
            link(split, fragment[0]);
            if (entry < 0)
              entry = split;
            else
              link(branch, split);
            branch = split;
          }
          yield new int[] { entry, exit };
        }
        case Repetition repetition -> {
          final int entry = newState(KIND_EPSILON);
          int exit = entry;
          for (int i = 0; i < repetition.min(); i++) {
            final int[] fragment = build(repetition.item());
            link(exit, fragment[0]);
            exit = fragment[1];
          }
          if (repetition.max() < 0) {
            // x* : a loop that can be left before or after any turn
            final int loop = newState(KIND_EPSILON);
            final int[] fragment = build(repetition.item());
            final int after = newState(KIND_EPSILON);
            link(exit, loop);
            link(loop, fragment[0]);
            link(loop, after);
            link(fragment[1], loop);
            exit = after;
          } else {
            // x? x? ... : each optional turn can be skipped straight to the end
            final int after = newState(KIND_EPSILON);
            for (int i = repetition.min(); i < repetition.max(); i++) {
              final int skip = newState(KIND_EPSILON);
              link(exit, skip);
              final int[] fragment = build(repetition.item());
              link(skip, fragment[0]);
              link(skip, after);
              exit = fragment[1];
            }
            link(exit, after);
            exit = after;
          }
          yield new int[] { entry, exit };
        }
      };
    }
  }

  /** A growable list of ints. */
  private static final class IntList {
    private int[] items = new int[16];
    private int   size;

    private void add(final int value) {
      if (size == items.length)
        items = Arrays.copyOf(items, size * 2);
      items[size++] = value;
    }

    private void addIfAbsent(final int value) {
      for (int i = 0; i < size; i++)
        if (items[i] == value)
          return;
      add(value);
    }

    private void copyFrom(final IntList other) {
      items = other.items;
      size = other.size;
    }

    private int[] toArray() {
      return Arrays.copyOf(items, size);
    }
  }
}
