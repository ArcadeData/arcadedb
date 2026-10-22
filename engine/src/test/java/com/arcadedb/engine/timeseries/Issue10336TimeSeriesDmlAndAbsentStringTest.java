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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10336: {@code DELETE FROM} a TIMESERIES type reported its matching samples as deleted and kept them, {@code UPDATE}
 * reported {@code count: 0} and {@code TRUNCATE TYPE} reported success, so the advice of the {@code ALTER PROPERTY} refusal ("delete
 * the non-conforming samples first") could not be followed. A sample is immutable: UPDATE is refused, naming what does work, and
 * since #10342 DELETE (by time range and tags) and TRUNCATE TYPE do remove samples. And a STRING column stores a sample with no value for it as the empty text, so a MIN or REGEXP accepted that sample
 * at write time and refused it once stored: the write path now judges the text the column will hold, as {@code ALTER} does, and
 * the outcome no longer depends on the order of the two.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10336TimeSeriesDmlAndAbsentStringTest extends TestHelper {
  private static final String SAMPLES = "[{\"t\":\"h\",\"s\":\"ab\",\"i\":1}, {\"t\":\"h\",\"s\":\"cd\",\"i\":2}]";

  @BeforeEach
  void seed() {
    database.command("sql", "CREATE TIMESERIES TYPE D1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
    database.command("sql", "INSERT INTO D1 SET ts = 1000, t = 'h', s = 'ab', i = 1");
    database.command("sql", "INSERT INTO D1 SET ts = 2000, t = 'h', s = 'cd', i = 2");
  }

  private String rows(final String type) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT t, s, i FROM " + type + " ORDER BY ts")) {
      while (rs.hasNext())
        rows.add(rs.next().toJSON().toString());
    }
    return rows.toString();
  }

  /**
   * Since #10342 a DELETE by time range and tags removes samples; what it cannot name exactly (a predicate on a FIELD column) is still
   * refused, and keeps every sample, as the DELETE that used to report them deleted did not.
   */
  @Test
  void deleteWithAPredicateItCannotNameExactlyIsRefusedAndKeepsTheSamples() {
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE i = 1").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("TIMESERIES type 'D1'").hasMessageContaining("DELETE");
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE ts = 1000 OR t = 'h'").close())
        .isInstanceOf(CommandExecutionException.class);
    assertThat(rows("D1")).isEqualTo(SAMPLES);
  }

  @Test
  void deleteByTimeRemovesTheSampleAndReportsIt() {
    try (final ResultSet rs = database.command("sql", "DELETE FROM D1 WHERE ts = 1000")) {
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
    }
    assertThat(rows("D1")).isEqualTo("[{\"t\":\"h\",\"s\":\"cd\",\"i\":2}]");
  }

  @Test
  void updateIsRefusedAndKeepsTheSamples() {
    assertThatThrownBy(() -> database.command("sql", "UPDATE D1 SET i = 6 WHERE ts = 2000").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("TIMESERIES type 'D1'").hasMessageContaining("UPDATE");
    assertThatThrownBy(() -> database.command("sql", "UPDATE D1 SET i = 6 UPSERT WHERE ts = 3000").close())
        .isInstanceOf(CommandExecutionException.class);
    assertThat(rows("D1")).isEqualTo(SAMPLES);
  }

  /** Since #10342 TRUNCATE TYPE removes the samples (it used to report success and keep them). */
  @Test
  void truncateTypeRemovesTheSamples() {
    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE D1")) {
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(2L);
    }
    assertThat(rows("D1")).isEqualTo("[]");
  }

  @Test
  void deleteFromASubqueryOfSamplesDoesNotCountWhatItCannotDelete() {
    // THE ROWS OF A TIMESERIES SCAN ARE NOT RECORDS: NOTHING TO DELETE, SO NOTHING MAY BE REPORTED DELETED
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM (SELECT FROM D1)").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
    assertThat(rows("D1")).isEqualTo(SAMPLES);
  }

  @Test
  void deleteFromASubqueryOfProjectionsOfRecordsIsRefusedToo() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'"));
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "DELETE FROM (SELECT name FROM Doc)").close()))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM Doc")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }
    // THE RECORDS THEMSELVES ARE DELETED AS BEFORE
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM (SELECT FROM Doc)")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
      }
    });
  }

  @Test
  void updateOverASubqueryOfRowsThatAreNotRecordsIsRefused() {
    // THE ROWS OF A TIMESERIES SCAN: NOTHING TO WRITE TO, WHICH USED TO BE REPORTED AS count 0
    assertThatThrownBy(() -> database.command("sql", "UPDATE (SELECT FROM D1) SET i = 6").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
    assertThat(rows("D1")).isEqualTo(SAMPLES);

    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'"));
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE (SELECT name FROM Doc) SET x = 1").close()))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a record");
    // A PROJECTION THAT CARRIES THE @rid OF A RECORD WRITES TO THAT RECORD, AS BEFORE
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE (SELECT @rid FROM Doc) SET x = 1")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT x FROM Doc")) {
      assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
    }
  }

  @Test
  void deleteOverASubqueryOfRidsDeletesTheRecords() {
    // THE SHAPE UPDATE ALREADY HONOURED: THE @rid OF THE ROW NAMES THE RECORD. DELETE USED TO COUNT IT AND DELETE NOTHING
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET name = 'a'");
      database.command("sql", "INSERT INTO Doc SET name = 'b'");
    });
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM (SELECT @rid FROM Doc WHERE name = 'a')")) {
        assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT name FROM Doc")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void theAlterRefusalNamesARemedyThatWorks() {
    database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', s = 'x', i = 3");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY D1.s MIN 2").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("timestamp 3000")
        .hasMessageContaining("Delete the non-conforming samples first").hasMessageContaining("DELETE FROM D1 WHERE ts = 3000");

    // THE REMEDY THE MESSAGE NAMES (issue #10342): THE SAMPLE IS DELETED BY ITS TIME, AND THE CONSTRAINT CAN BE DECLARED
    database.command("sql", "DELETE FROM D1 WHERE ts = 3000").close();
    database.command("sql", "ALTER PROPERTY D1.s MIN 2").close();
    assertThat(rows("D1")).isEqualTo(SAMPLES);
  }

  /** The other way out, which needs no delete: a new type that declares the constraint, and the conforming samples copied into it. */
  @Test
  void theConformingSamplesCanBeCopiedIntoANewType() {
    database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', s = 'x', i = 3");
    database.command("sql", "CREATE TIMESERIES TYPE D2 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
    database.command("sql", "ALTER PROPERTY D2.s MIN 2");
    database.command("sql", "INSERT INTO D2 FROM SELECT ts, t, s, i FROM D1 WHERE s.length() >= 2");
    assertThat(rows("D2")).isEqualTo(SAMPLES);
  }

  @Test
  void anAbsentStringIsJudgedAsTheEmptyTextWhicheverComesFirst() {
    // THE REPORTED ORDER: THE SAMPLE FIRST, THEN THE CONSTRAINT
    database.command("sql", "CREATE TIMESERIES TYPE W1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
    database.command("sql", "INSERT INTO W1 SET ts = 1000, t = 'h', i = 1");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W1.s MIN 2").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("empty text");
    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY W1.s REGEXP '[a-z]+'").close())
        .isInstanceOf(CommandExecutionException.class);

    // THE OTHER ORDER: THE CONSTRAINT FIRST, THEN THE SAME SAMPLE, NOW REFUSED THE SAME WAY
    database.command("sql", "CREATE TIMESERIES TYPE A1 TIMESTAMP ts TAGS (t STRING) FIELDS (s STRING, i INTEGER)");
    database.command("sql", "ALTER PROPERTY A1.s MIN 2");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', i = 1").close())
        .isInstanceOf(ValidationException.class).hasMessageContaining("empty text").hasMessageContaining("MIN 2");
    database.command("sql", "ALTER PROPERTY A1.s MIN null");
    database.command("sql", "ALTER PROPERTY A1.s REGEXP '[a-z]+'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', i = 1").close())
        .isInstanceOf(ValidationException.class).hasMessageContaining("REGEXP");
    assertThat(rows("A1")).isEqualTo("[]");

    // A VALUE THAT SATISFIES THEM IS STILL TAKEN, AND A CONSTRAINT THE EMPTY TEXT SATISFIES TAKES AN ABSENT VALUE
    database.command("sql", "INSERT INTO A1 SET ts = 1000, t = 'h', s = 'ab', i = 1");
    database.command("sql", "ALTER PROPERTY A1.s REGEXP '[a-z]*'");
    database.command("sql", "ALTER PROPERTY A1.s MAX 5");
    database.command("sql", "INSERT INTO A1 SET ts = 2000, t = 'h', i = 2");
    assertThat(rows("A1")).isEqualTo("[{\"t\":\"h\",\"s\":\"ab\",\"i\":1}, {\"t\":\"h\",\"s\":\"\",\"i\":2}]");
  }

  @Test
  void anAbsentTagIsJudgedAsTheEmptyTextToo() {
    database.command("sql", "CREATE TIMESERIES TYPE T1 TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
    database.command("sql", "ALTER PROPERTY T1.t MIN 1");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T1 SET ts = 1000, i = 1").close())
        .isInstanceOf(ValidationException.class);
    database.command("sql", "INSERT INTO T1 SET ts = 1000, t = 'h', i = 1");
    assertThat(rows("T1")).isEqualTo("[{\"t\":\"h\",\"s\":null,\"i\":1}]");
  }

  @Test
  void anAbsentNumberIsStillNoValue() {
    // A NUMERIC COLUMN KEEPS ITS NULL BIT: NO BOUND APPLIES TO NO VALUE, ON EITHER SIDE
    database.command("sql", "CREATE TIMESERIES TYPE N1 TIMESTAMP ts FIELDS (s STRING, i INTEGER)");
    database.command("sql", "INSERT INTO N1 SET ts = 1000, s = 'ab'");
    database.command("sql", "ALTER PROPERTY N1.i MIN 5");
    database.command("sql", "INSERT INTO N1 SET ts = 2000, s = 'cd'");
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM N1")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
    }
  }
}
