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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #10931: the widened narrowing scan found {@code vector.dequantizeInt8} reading the codes of a raw list through
 * {@code Number.byteValue()}, so 200 became -56 (the opposite end of the band) and 1.5 became 1, silently. It now reads
 * them through the same strict reader as {@code vector.approxDistance} (#9776): an integer in [-128, 127] or a refusal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10931DequantizeInt8CodeRangeTest extends TestHelper {
  private final SQLFunctionVectorDequantizeInt8 dequantize = new SQLFunctionVectorDequantizeInt8();
  private       BasicCommandContext             context;

  @BeforeEach
  void setupContext() {
    context = new BasicCommandContext();
    context.setDatabase(database);
  }

  @Test
  void codesOutsideTheInt8RangeAreRefusedNotWrapped() {
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(200, 0), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { new Object[] { -129, 0 }, 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(300L), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("[-128, 127]");
  }

  @Test
  void fractionalCodesAreRefusedNotTruncated() {
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(1.5, 0), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(new BigDecimal("0.5")), 0.0, 1.0 },
        context)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
    // A FRACTION A DOUBLE CANNOT HOLD, AND NaN OR AN INFINITY, WHICH A CAST WOULD TURN INTO 0
    assertThatThrownBy(() -> dequantize.execute(null, null, null,
        new Object[] { List.of(new BigDecimal("1.00000000000000000001")), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of(Double.NaN), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
    assertThatThrownBy(() -> dequantize.execute(null, null, null,
        new Object[] { new Object[] { Float.POSITIVE_INFINITY }, 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("integers");
    assertThatThrownBy(() -> dequantize.execute(null, null, null, new Object[] { List.of("x"), 0.0, 1.0 }, context))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("must be numbers");
  }

  @Test
  void codesInsideTheRangeOfAnyNumberKindDequantizeAsBefore() {
    // -128 IS THE BOTTOM OF THE BAND, 127 THE TOP
    final float[] fromInts = (float[]) dequantize.execute(null, null, null, new Object[] { List.of(-128, 127), 0.0, 255.0 },
        context);
    final float[] fromMixed = (float[]) dequantize.execute(null, null, null,
        new Object[] { new Object[] { -128L, new BigDecimal("127") }, 0.0, 255.0 }, context);
    assertThat(fromInts[0]).isCloseTo(0f, within(0.001f));
    assertThat(fromInts[1]).isCloseTo(255f, within(0.001f));
    assertThat(fromMixed).containsExactly(fromInts);
  }

  @Test
  void theSqlCallRefusesAnOutOfRangeCode() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT vector.dequantizeInt8([200, 0], 0.0, 1.0) AS v")) {
        rs.next();
      }
    }).hasMessageContaining("[-128, 127]");
  }
}
