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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The work the catalog spends combining the arms of a UNION, INTERSECT or EXCEPT chain is charged to the statement's work budget,
 * and a long chain of arms costs what it reads rather than what its length squared would.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogSetOperationBudgetTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogSetOperationBudgetTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- the statements a client sends

  /** A long chain of arms of different rows is answered, whole and in order: its cost is what it reads, not the square of its length. */
  @Test
  void aLongChainOfDistinctUnionsIsAnswered() {
    final int arms = 300;
    final int rowsPerArm = 300;

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(chain(arms, rowsPerArm, "union"), database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(arms * rowsPerArm);
    assertThat(answer.rows.get(0).get("r")).isEqualTo(1L);
    assertThat(answer.rows.get(arms * rowsPerArm - 1).get("r")).isEqualTo((long) arms * rowsPerArm);
  }

  /** The same with ALL, which keeps every row of every arm. */
  @Test
  void aLongChainOfUnionAllIsAnswered() {
    final int arms = 300;
    final int rowsPerArm = 300;

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(chain(arms, rowsPerArm, "union all"), database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(arms * rowsPerArm);
  }

  /** Arms that repeat each other's rows keep one row of each, wherever the repetition is in the chain. */
  @Test
  void aLongChainOfArmsThatRepeatEachOtherKeepsOneRowOfEach() {
    final StringBuilder query = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      if (i > 0)
        query.append(" union ");
      query.append("select r from generate_series(1, 300) as s(r)");
    }

    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query.toString(), database, USER);

    assertThat(answer).isNotNull();
    assertThat(answer.rows).hasSize(300);
  }

  /**
   * A chain that has to start over at every other step spends more than the statement may. One that names no catalog relation is
   * handed back to the SQL engine, as any set operation over the budget is.
   */
  @Test
  void aChainThatStartsOverAtEveryOtherStepIsHandedBack() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(alternatingChain(300, 400, ""), database, USER);

    assertThat(answer).isNull();
  }

  /** One that names a catalog relation is refused, rather than answered with fewer rows. */
  @Test
  void aChainOverACatalogRelationThatStartsOverAtEveryOtherStepIsRefused() {
    final String first = "select s.r from pg_database d, generate_series(1, 400) as s(r)";

    assertThatThrownBy(() -> PostgresCatalog.resolve(alternatingChain(300, 400, first), database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);
  }

  /** An ORDER BY over a set operation is charged by the comparisons it makes, which the rows it sorts decide. */
  @Test
  void theOrderByOfASetOperationIsCharged() {
    // 50 arms of 4000 rows: the 200,000 rows the arms read, and three more passes over them to copy, combine and materialize them,
    // fit the budget of the statement; the sort of them is what takes it over
    final PostgresCatalog.Answer unsorted = PostgresCatalog.resolve(chain(50, 4_000, "union"), database, USER);
    assertThat(unsorted).isNotNull();
    assertThat(unsorted.rows).hasSize(200_000);

    final PostgresCatalog.Answer sorted = PostgresCatalog.resolve(chain(50, 4_000, "union") + " order by r desc", database, USER);
    assertThat(sorted).isNull();

    // a few rows sort within the budget, whatever their ORDER BY
    final PostgresCatalog.Answer few = PostgresCatalog.resolve(chain(5, 4, "union") + " order by r desc", database, USER);
    assertThat(few).isNotNull();
    assertThat(few.rows).hasSize(20);
    assertThat(few.rows.get(0).get("r")).isEqualTo(20L);
  }

  // ---------------------------------------------------------------- the combination of the rows

  /** Combining a chain of UNIONs charges about what it reads, and not more than a few passes over the rows. */
  @Test
  void aChainOfUnionsIsChargedByTheRowsItReads() {
    final int arms = 200;
    final int rowsPerArm = 100;

    final AtomicLong charged = new AtomicLong();
    final List<List<Object>> combined = PostgresCatalogRowSets.combine(operators(arms - 1, "UNION"), all(arms - 1, false),
        distinctArms(arms, rowsPerArm), charged::addAndGet);

    assertThat(combined).hasSize(arms * rowsPerArm);
    assertThat(charged.get()).isBetween(1L, 3L * arms * rowsPerArm);
  }

  /** The same for a chain of UNION ALL. */
  @Test
  void aChainOfUnionAllIsChargedByTheRowsItReads() {
    final int arms = 200;
    final int rowsPerArm = 100;

    final AtomicLong charged = new AtomicLong();
    final List<List<Object>> combined = PostgresCatalogRowSets.combine(operators(arms - 1, "UNION"), all(arms - 1, true),
        distinctArms(arms, rowsPerArm), charged::addAndGet);

    assertThat(combined).hasSize(arms * rowsPerArm);
    assertThat(charged.get()).isBetween(1L, 3L * arms * rowsPerArm);
  }

  /** A chain that starts over at every other step is charged what that costs, and stops when the budget does. */
  @Test
  void aChainThatStartsOverIsChargedWhatItCostsAndStops() {
    final int arms = 200;
    final int rowsPerArm = 100;
    final List<String> operators = new ArrayList<>();
    final List<Boolean> all = new ArrayList<>();
    for (int i = 0; i < arms - 1; i++) {
      operators.add("UNION");
      all.add(i % 2 == 0);
    }

    final AtomicLong charged = new AtomicLong();
    PostgresCatalogRowSets.combine(operators, all, distinctArms(arms, rowsPerArm), charged::addAndGet);
    // about a quarter of the square of the rows: far more than the rows themselves
    assertThat(charged.get()).isGreaterThan(10L * arms * rowsPerArm);

    final long limit = 2L * arms * rowsPerArm;
    final AtomicLong spent = new AtomicLong();
    assertThatThrownBy(() -> PostgresCatalogRowSets.combine(operators, all, distinctArms(arms, rowsPerArm), units -> {
      if (spent.addAndGet(units) > limit)
        throw new PostgresCatalogExpression.BudgetExceeded();
    })).isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);
    // the work is charged before it is done: the statement stopped within one step of the limit, and no step reads more than every row
    assertThat(spent.get()).isLessThanOrEqualTo(limit + (long) arms * rowsPerArm);
  }

  /** INTERSECT and EXCEPT steps are charged by the rows on both sides, before they run. */
  @Test
  void intersectAndExceptStepsAreCharged() {
    for (final String operator : List.of("INTERSECT", "EXCEPT")) {
      final int arms = 50;
      final int rowsPerArm = 1_000;
      final List<List<List<Object>>> input = new ArrayList<>();
      for (int a = 0; a < arms; a++) {
        final List<List<Object>> rows = new ArrayList<>();
        for (int r = 0; r < rowsPerArm; r++)
          rows.add(List.of((Object) (long) r));
        input.add(rows);
      }

      final AtomicLong spent = new AtomicLong();
      assertThatThrownBy(() -> PostgresCatalogRowSets.combine(operators(arms - 1, operator), all(arms - 1, false), input, units -> {
        if (spent.addAndGet(units) > 5_000)
          throw new PostgresCatalogExpression.BudgetExceeded();
      })).as(operator).isInstanceOf(PostgresCatalogExpression.BudgetExceeded.class);
    }
  }

  /**
   * Whatever the chain is made of, the rows are the ones a step-by-step combination of the arms gives, in the same order: INTERSECT
   * binds tighter than UNION and EXCEPT, which go from the left, duplicates are kept by ALL only, and numbers of different types
   * that are equal are the same row.
   */
  @Test
  void theRowsAreThoseOfAStepByStepCombination() {
    final Random random = new Random(20260101L);
    final String[] operatorNames = { "UNION", "INTERSECT", "EXCEPT" };

    for (int round = 0; round < 3_000; round++) {
      final int armCount = 2 + random.nextInt(6);
      final List<List<List<Object>>> arms = new ArrayList<>();
      for (int a = 0; a < armCount; a++) {
        final List<List<Object>> rows = new ArrayList<>();
        final int rowCount = random.nextInt(9);
        for (int r = 0; r < rowCount; r++) {
          final long value = random.nextInt(6);
          // the same number as a Long and as an Integer, which are one row to a set operation
          rows.add(List.of(random.nextBoolean() ? (Object) value : (Object) (int) value, random.nextInt(2) == 0 ? "a" : "b"));
        }
        arms.add(rows);
      }
      final List<String> operators = new ArrayList<>();
      final List<Boolean> all = new ArrayList<>();
      for (int i = 0; i < armCount - 1; i++) {
        operators.add(operatorNames[random.nextInt(operatorNames.length)]);
        all.add(random.nextBoolean());
      }

      final List<List<Object>> expected = stepByStep(operators, all, copyOf(arms));
      final List<List<List<Object>>> input = copyOf(arms);
      final List<List<Object>> actual = PostgresCatalogRowSets.combine(operators, all, input, units -> {
      });

      assertThat(actual).as("%s %s of %s", operators, all, arms).isEqualTo(expected);
      // the arms the caller handed over are left as they were
      assertThat(input).as("arms of %s %s", operators, all).isEqualTo(arms);
    }
  }

  // ---------------------------------------------------------------- helpers

  /** {@code arms} arms of {@code rowsPerArm} rows, no row in two arms, joined by {@code operator}. */
  private static String chain(final int arms, final int rowsPerArm, final String operator) {
    final StringBuilder query = new StringBuilder();
    for (int i = 0; i < arms; i++) {
      if (i > 0)
        query.append(' ').append(operator).append(' ');
      query.append(arm(i, rowsPerArm));
    }
    return query.toString();
  }

  /** Arms of different rows joined by UNION ALL and UNION in turn, after {@code first} when it is not empty. */
  private static String alternatingChain(final int arms, final int rowsPerArm, final String first) {
    final StringBuilder query = new StringBuilder();
    for (int i = 0; i < arms; i++) {
      if (i > 0)
        query.append(i % 2 == 1 ? " union all " : " union ");
      query.append(i == 0 && !first.isEmpty() ? first : arm(i, rowsPerArm));
    }
    return query.toString();
  }

  private static String arm(final int index, final int rowsPerArm) {
    return "select r from generate_series(" + (index * rowsPerArm + 1L) + ", " + ((index + 1L) * rowsPerArm) + ") as s(r)";
  }

  private static List<String> operators(final int count, final String operator) {
    final List<String> operators = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      operators.add(operator);
    return operators;
  }

  private static List<Boolean> all(final int count, final boolean all) {
    final List<Boolean> flags = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      flags.add(all);
    return flags;
  }

  private static List<List<List<Object>>> distinctArms(final int arms, final int rowsPerArm) {
    final List<List<List<Object>>> input = new ArrayList<>(arms);
    for (int a = 0; a < arms; a++) {
      final List<List<Object>> rows = new ArrayList<>(rowsPerArm);
      for (int r = 0; r < rowsPerArm; r++)
        rows.add(List.of((Object) ((long) a * rowsPerArm + r)));
      input.add(rows);
    }
    return input;
  }

  private static List<List<List<Object>>> copyOf(final List<List<List<Object>>> arms) {
    final List<List<List<Object>>> copy = new ArrayList<>(arms.size());
    for (final List<List<Object>> arm : arms)
      copy.add(new ArrayList<>(arm));
    return copy;
  }

  /** The combination of the arms one binary step at a time, each step over two whole lists: what the chain means. */
  private static List<List<Object>> stepByStep(final List<String> operators, final List<Boolean> all,
      final List<List<List<Object>>> arms) {
    final List<List<List<Object>>> terms = new ArrayList<>();
    final List<Integer> termOperators = new ArrayList<>();
    List<List<Object>> current = arms.get(0);
    for (int i = 0; i < operators.size(); i++) {
      if ("INTERSECT".equals(operators.get(i)))
        current = step("INTERSECT", all.get(i), current, arms.get(i + 1));
      else {
        terms.add(current);
        termOperators.add(i);
        current = arms.get(i + 1);
      }
    }
    terms.add(current);

    List<List<Object>> combined = terms.get(0);
    for (int t = 0; t < termOperators.size(); t++) {
      final int i = termOperators.get(t);
      combined = step(operators.get(i), all.get(i), combined, terms.get(t + 1));
    }
    return combined;
  }

  private static List<List<Object>> step(final String operator, final boolean all, final List<List<Object>> left,
      final List<List<Object>> right) {
    final List<List<Object>> result = new ArrayList<>();
    if ("UNION".equals(operator)) {
      if (all) {
        result.addAll(left);
        result.addAll(right);
        return result;
      }
      final Set<List<Object>> seen = new HashSet<>();
      for (final List<List<Object>> side : List.of(left, right))
        for (final List<Object> row : side)
          if (seen.add(PostgresCatalogRowSets.distinctKey(row)))
            result.add(row);
      return result;
    }

    final Map<List<Object>, Integer> inRight = new HashMap<>();
    for (final List<Object> row : right)
      inRight.merge(PostgresCatalogRowSets.distinctKey(row), 1, Integer::sum);

    final Set<List<Object>> emitted = new HashSet<>();
    final boolean intersect = "INTERSECT".equals(operator);
    for (final List<Object> row : left) {
      final List<Object> key = PostgresCatalogRowSets.distinctKey(row);
      final int count = inRight.getOrDefault(key, 0);
      if (all) {
        if (count > 0)
          inRight.put(key, count - 1);
        if (intersect == (count > 0))
          result.add(row);
      } else if (intersect == (count > 0) && emitted.add(key))
        result.add(row);
    }
    return result;
  }
}
