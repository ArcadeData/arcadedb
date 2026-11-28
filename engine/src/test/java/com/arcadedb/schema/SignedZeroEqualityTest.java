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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Negative zero is one value with zero (IEEE 754, and issue #8920 for the DOUBLE pairs): a FLOAT holding -0.0 must be equal to
 * a BYTE or SHORT holding 0 and to a FLOAT holding 0.0, on every operand order, as it is to an INTEGER, a LONG, a DOUBLE or a
 * decimal.
 */
class SignedZeroEqualityTest extends TestHelper {

  private static final Number[] ZEROS = { (byte) 0, (short) 0, 0, 0L, 0f, -0f, 0d, -0d, new BigDecimal("0"), new BigDecimal("0.0"), BigInteger.ZERO };

  @Test
  void everyPairOfZerosIsEqual() {
    for (final Number left : ZEROS)
      for (final Number right : ZEROS)
        assertThat(Type.numbersEqual(left, right)).as("%s:%s = %s:%s", left.getClass().getSimpleName(), left, right.getClass().getSimpleName(), right)
            .isTrue();
  }

  @Test
  void aFloatNegativeZeroIsFoundByAShortOrByteOperandInSql() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Z");
      type.createProperty("f", Type.FLOAT);
      type.createProperty("s", Type.SHORT);
      database.newDocument("Z").set("id", 1).set("f", -0.0f).set("s", (short) 0).save();
      database.newDocument("Z").set("id", 2).set("f", 0.0f).set("s", (short) 0).save();
    });

    assertThat(ids("SELECT id FROM Z WHERE f = s", null)).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM Z WHERE f = :x", (short) 0)).containsExactly(1, 2);
    assertThat(ids("SELECT id FROM Z WHERE f = :x", (byte) 0)).containsExactly(1, 2);
  }

  @Test
  void theIndexAndTheScanAgreeOnAFloatNegativeZero() {
    database.transaction(() -> {
      for (final String name : new String[] { "ZI", "ZS" }) {
        final DocumentType type = database.getSchema().createDocumentType(name);
        type.createProperty("f", Type.FLOAT);
        if (name.equals("ZI"))
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "f");
        database.newDocument(name).set("id", 1).set("f", -0.0f).save();
        database.newDocument(name).set("id", 2).set("f", 0.0f).save();
      }
    });

    for (final Object probe : new Object[] { (byte) 0, (short) 0, 0, 0L, 0f, -0f, 0d, -0d, new BigDecimal("0.0") })
      assertThat(ids("SELECT id FROM ZI WHERE f = :x", probe)).as("%s:%s", probe.getClass().getSimpleName(), probe)
          .isEqualTo(ids("SELECT id FROM ZS WHERE f = :x", probe)).containsExactly(1, 2);
  }

  private List<Integer> ids(final String sql, final Object parameter) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = parameter == null ? database.query("sql", sql) : database.query("sql", sql, Map.of("x", parameter))) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    ids.sort(null);
    return ids;
  }
}
