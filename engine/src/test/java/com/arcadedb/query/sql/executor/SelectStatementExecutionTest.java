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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class SelectStatementExecutionTest extends TestHelper {

  @Test
  void selectNoTarget() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Integer>getProperty("one")).isEqualTo(1);
    assertThat(item.<Integer>getProperty("two")).isEqualTo(2);
    assertThat(item.<Integer>getProperty("2 + 3")).isEqualTo(5);

    result.close();
  }

  @Test
  void groupByCount() {
    database.getSchema().createDocumentType("InputTx");

    database.begin();
    for (int i = 0; i < 100; i++) {
      final String hash = UUID.randomUUID().toString();
      database.command("sql", "insert into InputTx set address = '" + hash + "'");

      // CREATE RANDOM NUMBER OF COPIES final int random = new Random().nextInt(10);
      final int random = new Random().nextInt(10);
      for (int j = 0; j < random; j++) {
        database.command("sql", "insert into InputTx set address = '" + hash + "'");
      }
    }

    database.commit();
    final ResultSet result = database.query("sql",
        "select address, count(*) as occurrences from InputTx where address is not null group by address limit 10");
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(row.<String>getProperty("address")).isNotNull(); // <== FALSE!
      assertThat(row.<Long>getProperty("occurrences")).isNotNull();
    }
    result.close();
  }

  @Test
  void selectNoTargetSkip() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3 skip 1");
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void selectNoTargetSkipZero() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3 skip 0");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Integer>getProperty("one")).isEqualTo(1);
    assertThat(item.<Integer>getProperty("two")).isEqualTo(2);
    assertThat(item.<Integer>getProperty("2 + 3")).isEqualTo(5);

    result.close();
  }

  @Test
  void selectNoTargetLimit0() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3 limit 0");
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void selectNoTargetLimit1() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3 limit 1");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Integer>getProperty("one")).isEqualTo(1);
    assertThat(item.<Integer>getProperty("two")).isEqualTo(2);
    assertThat(item.<Integer>getProperty("2 + 3")).isEqualTo(5);

    result.close();
  }

  @Test
  void selectNoTargetLimitx() {
    final ResultSet result = database.query("sql", "select 1 as one, 2 as two, 2+3 skip 0 limit 0");
    result.close();
  }

  @Test
  void selectFullScan1() {
    final String className = "TestSelectFullScan1";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100000; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className);
    for (int i = 0; i < 100000; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectFullScanOrderByRidAsc() {
    final String className = "testSelectFullScanOrderByRidAsc";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100000; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " ORDER BY @rid ASC");
    Document lastItem = null;
    for (int i = 0; i < 100000; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
      if (lastItem != null) {
        assertThat(lastItem.getIdentity().compareTo(item.getElement().get().getIdentity()) < 0).isTrue();
      }
      lastItem = item.getElement().get();
    }
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void selectFullScanOrderByRidDesc() {
    final String className = "testSelectFullScanOrderByRidDesc";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100000; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " ORDER BY @rid DESC");
    Document lastItem = null;
    for (int i = 0; i < 100000; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
      if (lastItem != null) {
        assertThat(lastItem.getIdentity().compareTo(item.getElement().get().getIdentity()) > 0).isTrue();
      }
      lastItem = item.getElement().get();
    }
    assertThat(result.hasNext()).isFalse();

    result.close();
  }

  @Test
  void selectFullScanLimit1() {
    final String className = "testSelectFullScanLimit1";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 300; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " limit 10");

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectFullScanSkipLimit1() {
    final String className = "testSelectFullScanSkipLimit1";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 300; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " skip 100 limit 10");

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectOrderByDesc() {
    final String className = "testSelectOrderByDesc";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 30; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " order by surname desc");

    String lastSurname = null;
    for (int i = 0; i < 30; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final String thisSurname = item.<String>getProperty("surname");
      if (lastSurname != null) {
        assertThat(lastSurname.compareTo(thisSurname) >= 0).isTrue();
      }
      lastSurname = thisSurname;
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectOrderByAsc() {
    final String className = "testSelectOrderByAsc";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 30; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " order by surname asc");

    String lastSurname = null;
    for (int i = 0; i < 30; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final String thisSurname = item.<String>getProperty("surname");
      if (lastSurname != null) {
        assertThat(lastSurname.compareTo(thisSurname) <= 0).isTrue();
      }
      lastSurname = thisSurname;
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectOrderByMassiveAsc() {
    final String className = "testSelectOrderByMassiveAsc";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100000; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i % 100);
      doc.save();
    }
    database.commit();
    final long begin = System.nanoTime();
    final ResultSet result = database.query("sql", "select from " + className + " order by surname asc limit 100");
    //    System.out.println("elapsed: " + (System.nanoTime() - begin));

    for (int i = 0; i < 100; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isEqualTo("surname0");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectOrderWithProjections() {
    final String className = "testSelectOrderWithProjections";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 10);
      doc.set("surname", "surname" + i % 10);
      doc.save();
    }
    database.commit();
    final long begin = System.nanoTime();
    final ResultSet result = database.query("sql", "select name from " + className + " order by surname asc");
    //    System.out.println("elapsed: " + (System.nanoTime() - begin));

    String lastName = null;
    for (int i = 0; i < 100; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if (i > 0) {
        assertThat(name.compareTo(lastName) >= 0).isTrue();
      }
      lastName = name;
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectOrderWithProjections2() {
    final String className = "testSelectOrderWithProjections2";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 100; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 10);
      doc.set("surname", "surname" + i % 10);
      doc.save();
    }
    database.commit();
    final long begin = System.nanoTime();
    final ResultSet result = database.query("sql", "select name from " + className + " order by name asc, surname asc");
    //    System.out.println("elapsed: " + (System.nanoTime() - begin));

    String lastName = null;
    for (int i = 0; i < 100; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      assertThat(name).isNotNull();
      if (i > 0) {
        assertThat(name.compareTo(lastName) >= 0).isTrue();
      }
      lastName = name;
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectFullScanWithFilter1() {
    final String className = "testSelectFullScanWithFilter1";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 300; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' or name = 'name7' ");

    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object name = item.getProperty("name");
      assertThat("name1".equals(name) || "name7".equals(name)).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectFullScanWithFilter2() {
    final String className = "testSelectFullScanWithFilter2";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 300; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " where name <> 'name1' ");

    for (int i = 0; i < 299; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object name = item.getProperty("name");
      assertThat(name).isNotEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void projections() {
    final String className = "testProjections";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 300; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select name from " + className);

    for (int i = 0; i < 300; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final String name = item.getProperty("name");
      final String surname = item.<String>getProperty("surname");
      assertThat(name).isNotNull();
      assertThat(name.startsWith("name")).isTrue();
      assertThat(surname).isNull();
      assertThat(item.getElement().isPresent()).isFalse();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void countStar() {
    final String className = "testCountStar";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 7; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.save();
    }
    database.commit();
    try {
      final ResultSet result = database.query("sql", "select count(*) from " + className);

      assertThat(Optional.ofNullable(result)).isNotNull();
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat((Object) next.getProperty("count(*)")).isEqualTo(7L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    } catch (final Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  void countStar2() {
    final String className = "testCountStar2";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + (i % 5));
      doc.save();
    }
    database.commit();
    try {
      final ResultSet result = database.query("sql", "select count(*), name from " + className + " group by name");

      assertThat(Optional.ofNullable(result)).isNotNull();
      for (int i = 0; i < 5; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result next = result.next();
        assertThat(next).isNotNull();
        assertThat((Object) next.getProperty("count(*)")).isEqualTo(2L);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    } catch (final Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  void countStarEmptyNoIndex() {
    final String className = "testCountStarEmptyNoIndex";
    database.getSchema().createDocumentType(className);

    database.begin();
    final MutableDocument elem = database.newDocument(className);
    elem.set("name", "bar");
    elem.save();
    database.commit();

    try {
      final ResultSet result = database.query("sql", "select count(*) from " + className + " where name = 'foo'");

      assertThat(Optional.ofNullable(result)).isNotNull();
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat((Object) next.getProperty("count(*)")).isEqualTo(0L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    } catch (final Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  void countStarEmptyNoIndexWithAlias() {
    final String className = "testCountStarEmptyNoIndexWithAlias";
    database.getSchema().createDocumentType(className);

    database.begin();
    final MutableDocument elem = database.newDocument(className);
    elem.set("name", "bar");
    elem.save();
    database.commit();

    try {
      final ResultSet result = database.query("sql", "select count(*) as a from " + className + " where name = 'foo'");

      assertThat(Optional.ofNullable(result)).isNotNull();
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat((Object) next.getProperty("a")).isEqualTo(0L);
      assertThat(result.hasNext()).isFalse();
      result.close();
    } catch (final Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  void aggregateMixedWithNonAggregate() {
    final String className = "testAggregateMixedWithNonAggregate";
    database.getSchema().createDocumentType(className);

    try {
      database.query("sql", "select max(a) + max(b) + pippo + pluto as foo, max(d) + max(e), f from " + className).close();
      fail("");
    } catch (final CommandExecutionException x) {

    } catch (final Exception e) {
      fail("");
    }
  }

  @Test
  void aggregateMixedWithNonAggregateInCollection() {
    final String className = "testAggregateMixedWithNonAggregateInCollection";
    database.getSchema().createDocumentType(className);

    try {
      database.query("sql", "select [max(a), max(b), foo] from " + className).close();
      fail("");
    } catch (final CommandExecutionException x) {

    } catch (final Exception e) {
      fail("");
    }
  }

  @Test
  void aggregateInCollection() {
    final String className = "testAggregateInCollection";
    database.getSchema().createDocumentType(className);

    try {
      final String query = "select [max(a), max(b)] from " + className;
      final ResultSet result = database.query("sql", query);
      result.close();
    } catch (final Exception x) {
      fail("");
    }
  }

  @Test
  void aggregateMixedWithNonAggregateConstants() {
    final String className = "testAggregateMixedWithNonAggregateConstants";
    database.getSchema().createDocumentType(className);

    try {
      final ResultSet result = database.query("sql",
          "select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from " + className);

      result.close();
    } catch (final Exception e) {
      e.printStackTrace();
      fail("");
    }
  }

  @Test
  void aggregateSum() {
    final String className = "testAggregateSum";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select sum(val) from " + className);
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat((Object) item.getProperty("sum(val)")).isEqualTo(45);

    result.close();
  }

  @Test
  void aggregateSumGroupBy() {
    final String className = "testAggregateSumGroupBy";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("type", i % 2 == 0 ? "even" : "odd");
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select sum(val), type from " + className + " group by type");
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      if ("even".equals(item.getProperty("type"))) {
        assertThat(item.<Object>getProperty("sum(val)")).isEqualTo(20);
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        assertThat(item.<Object>getProperty("sum(val)")).isEqualTo(25);
        oddFound = true;
      }
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(evenFound).isTrue();
    assertThat(oddFound).isTrue();
    result.close();
  }

  @Test
  void aggregateSumMaxMinGroupBy() {
    final String className = "testAggregateSumMaxMinGroupBy";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("type", i % 2 == 0 ? "even" : "odd");
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql",
        "select sum(val), max(val), min(val), type from " + className + " group by type");
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      if ("even".equals(item.getProperty("type"))) {
        assertThat(item.<Object>getProperty("sum(val)")).isEqualTo(20);
        assertThat(item.<Object>getProperty("max(val)")).isEqualTo(8);
        assertThat(item.<Object>getProperty("min(val)")).isEqualTo(0);
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        assertThat(item.<Object>getProperty("sum(val)")).isEqualTo(25);
        assertThat(item.<Object>getProperty("max(val)")).isEqualTo(9);
        assertThat(item.<Object>getProperty("min(val)")).isEqualTo(1);
        oddFound = true;
      }
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(evenFound).isTrue();
    assertThat(oddFound).isTrue();
    result.close();
  }

  @Test
  void aggregateSumNoGroupByInProjection() {
    final String className = "testAggregateSumNoGroupByInProjection";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("type", i % 2 == 0 ? "even" : "odd");
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select sum(val) from " + className + " group by type");
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object sum = item.getProperty("sum(val)");
      if (sum.equals(20)) {
        evenFound = true;
      } else if (sum.equals(25)) {
        oddFound = true;
      }
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(evenFound).isTrue();
    assertThat(oddFound).isTrue();
    result.close();
  }

  @Test
  void aggregateSumNoGroupByInProjection2() {
    final String className = "testAggregateSumNoGroupByInProjection2";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("type", i % 2 == 0 ? "dd1" : "dd2");
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select sum(val) from " + className + " group by type.substring(0,1)");
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object sum = item.getProperty("sum(val)");
      assertThat(sum).isEqualTo(45);
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromBucketNumber() {
    final String className = "testFetchFromBucketNumber";
    final Schema schema = database.getSchema();
    final DocumentType clazz = schema.createDocumentType(className);
    final String targetClusterName = clazz.getBuckets(false).getFirst().getName();

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save(targetClusterName);
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from bucket:" + targetClusterName);
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Integer val = item.getProperty("val");
      assertThat(val).isNotNull();
      sum += val;
    }
    assertThat(sum).isEqualTo(45);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromBucketNumberOrderByRidDesc() {
    final String className = "testFetchFromBucketNumberOrderByRidDesc";
    final Schema schema = database.getSchema();
    final DocumentType clazz = schema.createDocumentType(className);

    final String targetBucketName = clazz.getBuckets(false).getFirst().getName();

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save(targetBucketName);
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from bucket:" + targetBucketName + " order by @rid desc");
    final int sum = 0;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Integer val = item.getProperty("val");
      assertThat(9 - val).isEqualTo(i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClusterNumberOrderByRidAsc() {
    final String className = "testFetchFromClusterNumberOrderByRidAsc";
    final Schema schema = database.getSchema();
    final DocumentType clazz = schema.createDocumentType(className);

    final String targetClusterName = clazz.getBuckets(false).getFirst().getName();

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save(targetClusterName);
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from bucket:" + targetClusterName + " order by @rid asc");
    final int sum = 0;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Integer val = item.getProperty("val");
      assertThat(val).isEqualTo((Object) i);
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClustersNumberOrderByRidAsc() {
    final String className = "testFetchFromClustersNumberOrderByRidAsc";
    final Schema schema = database.getSchema();
    final DocumentType clazz = schema.createDocumentType(className);
    if (clazz.getBuckets(false).size() < 2) {
      //clazz.addCluster("testFetchFromClustersNumberOrderByRidAsc_2");
      return;
    }

    final String targetClusterName = clazz.getBuckets(false).getFirst().getName();
    final String targetClusterName2 = clazz.getBuckets(false).get(1).getName();

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save(targetClusterName);
    }
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save(targetClusterName2);
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from bucket:[" + targetClusterName + ", " + targetClusterName2 + "] order by @rid asc");

    for (int i = 0; i < 20; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Integer val = item.getProperty("val");
      assertThat(val).isEqualTo((Object) (i % 10));
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void queryAsTarget() {
    final String className = "testQueryAsTarget";
    final Schema schema = database.getSchema();
    final DocumentType clazz = schema.createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("val", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from (select from " + className + " where val > 2)  where val < 8");

    for (int i = 0; i < 5; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      final Integer val = item.getProperty("val");
      assertThat(val > 2).isTrue();
      assertThat(val < 8).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void querySchema() {
    final DocumentType type = database.getSchema().createDocumentType("testQuerySchema");
    type.setCustomValue("description", "this is just a test");

    final ResultSet result = database.query("sql", "select from schema:types");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("name")).isEqualTo("testQuerySchema");

    final Map<String, Object> customType = item.getProperty("custom");
    assertThat(customType).isNotNull();
    assertThat(customType.size()).isEqualTo(1);

    assertThat(customType.get("description")).isEqualTo("this is just a test");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void queryMetadataIndexManager() {
    final DocumentType type = database.getSchema().createDocumentType("testQuerySchema");
    database.begin();
    type.createProperty("name", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    database.commit();
    final ResultSet result = database.query("sql", "select from schema:indexes");

    while (result.hasNext()) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item.<String>getProperty("name")).isNotNull();
      assertThat(item.<List<String>>getProperty("keyTypes")).first().isEqualTo("STRING");
      assertThat(item.<Boolean>getProperty("unique")).isFalse();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void queryMetadataDatabase() {
    final ResultSet result = database.query("sql", "select from schema:database");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("name")).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void nonExistingRids() {
    final int bucketId = database.getSchema().createDocumentType("testNonExistingRids").getBuckets(false).getFirst().getFileId();
    final ResultSet result = database.query("sql", "select from #" + bucketId + ":100000000");
    assertThat(result.hasNext()).isTrue();

    try {
      result.next();
    } catch (RecordNotFoundException e) {
    }

    result.close();
  }

  @Test
  void fetchFromSingleRid() {
    database.getSchema().createDocumentType("testFetchFromSingleRid");
    database.begin();
    final MutableDocument doc = database.newDocument("testFetchFromSingleRid");
    doc.save();
    database.commit();
    final ResultSet result = database.query("sql", "select from #1:0");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSingleRid2() {
    database.getSchema().createDocumentType("testFetchFromSingleRid2");
    database.begin();
    final MutableDocument doc = database.newDocument("testFetchFromSingleRid2");
    doc.save();
    database.commit();
    final ResultSet result = database.query("sql", "select from [#1:0]");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSingleRidParam() {
    database.getSchema().createDocumentType("testFetchFromSingleRidParam");
    database.begin();
    final MutableDocument doc = database.newDocument("testFetchFromSingleRidParam");
    doc.save();
    database.commit();
    final ResultSet result = database.query("sql", "select from ?", new RID(database, 1, 0));
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSingleRid3() {
    database.getSchema().createDocumentType("testFetchFromSingleRid3", 8);
    database.begin();
    MutableDocument doc = database.newDocument("testFetchFromSingleRid3");
    doc.save();
    doc = database.newDocument("testFetchFromSingleRid3");
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select from [#1:0, #2:0]");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSingleRid4() {
    database.getSchema().createDocumentType("testFetchFromSingleRid4", 8);
    database.begin();
    MutableDocument doc = database.newDocument("testFetchFromSingleRid4");
    doc.save();
    doc = database.newDocument("testFetchFromSingleRid4");
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select from [#1:0, #2:0, #1:100000]");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next()).isNotNull();

    assertThat(result.hasNext()).isTrue();
    try {
      result.next();
    } catch (RecordNotFoundException e) {
    }
    result.close();
  }

  @Test
  void fetchFromClassWithIndex() {
    final String className = "testFetchFromClassWithIndex";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name2'");

    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next).isNotNull();
    assertThat(next.<String>getProperty("name")).isEqualTo("name2");

    assertThat(result.hasNext()).isFalse();

    final Optional<ExecutionPlan> p = result.getExecutionPlan();
    assertThat(p.isPresent()).isTrue();
    final ExecutionPlan p2 = p.get();
    assertThat(p2 instanceof SelectExecutionPlan).isTrue();
    final SelectExecutionPlan plan = (SelectExecutionPlan) p2;
    assertThat(plan.getSteps().getFirst().getClass()).isEqualTo(FetchFromIndexStep.class);
    result.close();
  }

  @Test
  void fetchFromIndex() {
    final String className = "testFetchFromIndex";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    final String indexName;
    final Index idx = clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    indexName = idx.getName();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from index:`" + indexName + "` where key = 'name2'");

    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next).isNotNull();

    assertThat(result.hasNext()).isFalse();

    final Optional<ExecutionPlan> p = result.getExecutionPlan();
    assertThat(p.isPresent()).isTrue();
    final ExecutionPlan p2 = p.get();
    assertThat(p2 instanceof SelectExecutionPlan).isTrue();
    final SelectExecutionPlan plan = (SelectExecutionPlan) p2;
    assertThat(plan.getSteps().getFirst().getClass()).isEqualTo(FetchFromIndexStep.class);
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes() {
    final String className = "testFetchFromClassWithIndexes";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name2' or surname = 'surname3'");

    assertThat(result.hasNext()).isTrue();
    for (int i = 0; i < 2; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname")))).isTrue();
    }

    assertThat(result.hasNext()).isFalse();

    final Optional<ExecutionPlan> p = result.getExecutionPlan();
    assertThat(p.isPresent()).isTrue();
    final ExecutionPlan p2 = p.get();
    assertThat(p2 instanceof SelectExecutionPlan).isTrue();
    final SelectExecutionPlan plan = (SelectExecutionPlan) p2;
    assertThat(plan.getSteps().getFirst().getClass()).isEqualTo(ParallelExecStep.class);
    final ParallelExecStep parallel = (ParallelExecStep) plan.getSteps().getFirst();
    assertThat(parallel.getSubExecutionPlans().size()).isEqualTo(2);
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes2() {
    final String className = "testFetchFromClassWithIndexes2";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    database.commit();
    final ResultSet result = database.query("sql",
        "select from " + className + " where foo is not null and (name = 'name2' or surname = 'surname3')");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes3() {
    final String className = "testFetchFromClassWithIndexes3";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where foo < 100 and (name = 'name2' or surname = 'surname3')");

    assertThat(result.hasNext()).isTrue();
    for (int i = 0; i < 2; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname")))).isTrue();
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes4() {
    final String className = "testFetchFromClassWithIndexes4";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className
        + " where foo < 100 and ((name = 'name2' and foo < 20) or surname = 'surname3') and ( 4<5 and foo < 50)");

    assertThat(result.hasNext()).isTrue();
    for (int i = 0; i < 2; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname")))).isTrue();
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes5() {
    final String className = "testFetchFromClassWithIndexes5";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name3' and surname >= 'surname1'");

    assertThat(result.hasNext()).isTrue();
    for (int i = 0; i < 1; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat(next.<String>getProperty("name")).isEqualTo("name3");
    }

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes6() {
    final String className = "testFetchFromClassWithIndexes6";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name3' and surname > 'surname3'");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes7() {
    final String className = "testFetchFromClassWithIndexes7";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name3' and surname >= 'surname3'");
    for (int i = 0; i < 1; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat(next.<String>getProperty("name")).isEqualTo("name3");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes8() {
    final String className = "testFetchFromClassWithIndexes8";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name3' and surname < 'surname3'");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes9() {
    final String className = "testFetchFromClassWithIndexes9";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name3' and surname <= 'surname3'");
    for (int i = 0; i < 1; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
      assertThat(next.<String>getProperty("name")).isEqualTo("name3");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes10() {
    final String className = "testFetchFromClassWithIndexes10";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name > 'name3' ");
    for (int i = 0; i < 6; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes11() {
    final String className = "testFetchFromClassWithIndexes11";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name >= 'name3' ");
    for (int i = 0; i < 7; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes12() {
    final String className = "testFetchFromClassWithIndexes12";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name < 'name3' ");
    for (int i = 0; i < 3; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes13() {
    final String className = "testFetchFromClassWithIndexes13";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name <= 'name3' ");
    for (int i = 0; i < 4; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes14() {
    final String className = "testFetchFromClassWithIndexes14";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name > 'name3' and name < 'name5'");
    for (int i = 0; i < 1; i++) {
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    final SelectExecutionPlan plan = (SelectExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    result.close();
  }

  @Test
  void fetchFromClassWithIndexes15() {
    final String className = "testFetchFromClassWithIndexes15";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "surname");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where name > 'name6' and name = 'name3' and surname > 'surname2' and surname < 'surname5' ");
    assertThat(result.hasNext()).isFalse();
    final SelectExecutionPlan plan = (SelectExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    result.close();
  }

//    @Test
//    public void testFetchFromClassWithHashIndexes1() {
//        String className = "testFetchFromClassWithHashIndexes1";
//        DocumentType clazz = database.getSchema().createDocumentType(className);
//        clazz.createProperty("name", Type.STRING);
//        clazz.createProperty("surname", Type.STRING);
//        clazz.createTypeIndex(
//                className + ".name_surname", Schema.INDEX_TYPE.LSM_TREE, false_HASH_INDEX, "name", "surname");
//
//        for (int i = 0; i < 10; i++) {
//            MutableDocument doc = database.newDocument(className);
//            doc.set("name", "name" + i);
//            doc.set("surname", "surname" + i);
//            doc.set("foo", i);
//            doc.save();
//        }
//
//        ResultSet result =
//                database.query("sql", "select from " + className + " where name = 'name6' and surname = 'surname6' ");
//
//
//        for (int i = 0; i < 1; i++) {
//            Assertions.assertThat(result.hasNext()).isTrue();
//            Result next = result.next();
//            Assertions.assertThat(next).isNotNull();
//        }
//        Assertions.assertThat(result.hasNext()).isFalse();
//        SelectExecutionPlan plan = (SelectExecutionPlan) result.getExecutionPlan().get();
//        Assertions.assertEquals(
//                1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
//        result.close();
//    }
//
//    @Test
//    public void testFetchFromClassWithHashIndexes2() {
//        String className = "testFetchFromClassWithHashIndexes2";
//        DocumentType clazz = database.getSchema().createDocumentType(className);
//        clazz.createProperty("name", Type.STRING);
//        clazz.createProperty("surname", Type.STRING);
//        clazz.createTypeIndex(
//                className + ".name_surname", Schema.INDEX_TYPE.LSM_TREE, false_HASH_INDEX, "name", "surname");
//
//        for (int i = 0; i < 10; i++) {
//            MutableDocument doc = database.newDocument(className);
//            doc.set("name", "name" + i);
//            doc.set("surname", "surname" + i);
//            doc.set("foo", i);
//            doc.save();
//        }
//
//        ResultSet result =
//                database.query("sql", "select from " + className + " where name = 'name6' and surname >= 'surname6' ");
//
//
//        for (int i = 0; i < 1; i++) {
//            Assertions.assertThat(result.hasNext()).isTrue();
//            Result next = result.next();
//            Assertions.assertThat(next).isNotNull();
//        }
//        Assertions.assertThat(result.hasNext()).isFalse();
//        SelectExecutionPlan plan = (SelectExecutionPlan) result.getExecutionPlan().get();
//        Assertions.assertEquals(
//                FetchFromClassExecutionStep.class, plan.getSteps().get(0).getClass()); // index not used
//        result.close();
//    }

  @Test
  void expand1() {
    final String childClassName = "testExpand1_child";
    final String parentClassName = "testExpand1_parent";
    final DocumentType childClass = database.getSchema().createDocumentType(childClassName);
    final DocumentType parentClass = database.getSchema().createDocumentType(parentClassName);

    database.begin();
    final int count = 10;
    for (int i = 0; i < count; i++) {
      final MutableDocument doc = database.newDocument(childClassName);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.set("foo", i);
      doc.save();

      final MutableDocument parent = database.newDocument(parentClassName);
      parent.set("linked", doc);
      parent.save();
    }
    database.commit();

    ResultSet result = database.query("sql", "select expand(linked) from " + parentClassName);
    for (int i = 0; i < count; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();

    assertThatThrownBy(
        () -> database.query("sql", "select expand(linked).asString() from " + parentClassName)).isInstanceOf(
        CommandSQLParsingException.class);

    assertThatThrownBy(
        () -> database.query("sql", "SELECT expand([{'name':2},2,3,4]).name from " + parentClassName)).isInstanceOf(
        CommandSQLParsingException.class);

    result.close();
  }

  @Test
  void expand2() {
    final String childClassName = "testExpand2_child";
    final String parentClassName = "testExpand2_parent";
    final DocumentType childClass = database.getSchema().createDocumentType(childClassName);
    final DocumentType parentClass = database.getSchema().createDocumentType(parentClassName);

    final int count = 10;
    final int collSize = 11;
    database.begin();
    for (int i = 0; i < count; i++) {
      final List coll = new ArrayList();
      for (int j = 0; j < collSize; j++) {
        final MutableDocument doc = database.newDocument(childClassName);
        doc.set("name", "name" + i);
        doc.save();
        coll.add(doc);
      }

      final MutableDocument parent = database.newDocument(parentClassName);
      parent.set("linked", coll);
      parent.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select expand(linked) from " + parentClassName);

    for (int i = 0; i < count * collSize; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void expand3() {
    final String childClassName = "testExpand3_child";
    final String parentClassName = "testExpand3_parent";
    final DocumentType childClass = database.getSchema().createDocumentType(childClassName);
    final DocumentType parentClass = database.getSchema().createDocumentType(parentClassName);

    final int count = 30;
    final int collSize = 7;
    database.begin();
    for (int i = 0; i < count; i++) {
      final List coll = new ArrayList<>();
      for (int j = 0; j < collSize; j++) {
        final MutableDocument doc = database.newDocument(childClassName);
        doc.set("name", "name" + j);
        doc.save();
        coll.add(doc);
      }

      final MutableDocument parent = database.newDocument(parentClassName);
      parent.set("linked", coll);
      parent.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select expand(linked) from " + parentClassName + " order by name");

    String last = null;
    for (int i = 0; i < count * collSize; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      if (i > 0) {
        assertThat(last.compareTo(next.getProperty("name")) <= 0).isTrue();
      }
      last = next.getProperty("name");
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void distinct1() {
    final String className = "testDistinct1";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    for (int i = 0; i < 30; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 10);
      doc.set("surname", "surname" + i % 10);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select distinct name, surname from " + className);

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void distinct2() {
    final String className = "testDistinct2";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);

    for (int i = 0; i < 30; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 10);
      doc.set("surname", "surname" + i % 10);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select distinct(name) from " + className);

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void let1() {
    final ResultSet result = database.query("sql", "select $a as one, $b as two let $a = 1, $b = 1+1");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Object>getProperty("one")).isEqualTo(1);
    assertThat(item.<Object>getProperty("two")).isEqualTo(2);
    result.close();
  }

  @Test
  void let1Long() {
    final ResultSet result = database.query("sql", "select $a as one, $b as two let $a = 1L, $b = 1L+1");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<Object>getProperty("one")).isEqualTo(1l);
    assertThat(item.<Object>getProperty("two")).isEqualTo(2l);
    result.close();
  }

  @Test
  void let2() {
    final ResultSet result = database.query("sql", "select $a as one let $a = (select 1 as a)");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Object one = item.getProperty("one");
    assertThat(one instanceof List).isTrue();
    assertThat(((List) one).size()).isEqualTo(1);
    final Object x = ((List) one).getFirst();
    assertThat(x instanceof Result).isTrue();
    assertThat((Object) ((Result) x).getProperty("a")).isEqualTo(1);
    result.close();
  }

  @Test
  void let3() {
    final ResultSet result = database.query("sql", "select $a[0].foo as one let $a = (select 1 as foo)");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    final Object one = item.getProperty("one");
    assertThat(one).isEqualTo(1);
    result.close();
  }

  @Test
  void let4() {
    final String className = "testLet4";
    database.getSchema().createDocumentType(className);
    database.begin();

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select name, surname, $nameAndSurname as fullname from " + className + " let $nameAndSurname = name + ' ' + surname");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.getProperty("name") + " " + item.<String>getProperty("surname")).isEqualTo(item.getProperty("fullname"));
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void let5() {
    final String className = "testLet5";
    database.getSchema().createDocumentType(className);
    database.begin();

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where name in (select name from " + className + " where name = 'name1')");
    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void let6() {
    final String className = "testLet6";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select $foo as name from " + className + " let $foo = (select name from " + className
            + " where name = $parent.$current.name)");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<List<String>>getProperty("name")).isNotNull();
      assertThat(item.getProperty("name") instanceof Collection).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void let7() {
    final String className = "testLet7";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select $bar as name from " + className + " " + "let $foo = (select name from " + className
            + " where name = $parent.$current.name)," + "$bar = $foo[0].name");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      assertThat(item.getProperty("name") instanceof String).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  //    @Test
  public void testLetWithTraverseFunction() {
    final String vertexClassName = "testLetWithTraverseFunction";
    final String edgeClassName = "testLetWithTraverseFunctioEdge";
    database.begin();

    final DocumentType vertexClass = database.getSchema().createVertexType(vertexClassName);

    final MutableVertex doc1 = database.newVertex(vertexClassName);
    doc1.set("name", "A");
    doc1.save();

    final MutableVertex doc2 = database.newVertex(vertexClassName);
    doc2.set("name", "B");
    doc2.save();
    final RID doc2Id = doc2.getIdentity();

    final DocumentType edgeClass = database.getSchema().createEdgeType(edgeClassName);

    doc1.newEdge(edgeClassName, doc2).save();
    database.commit();

    final String queryString = "SELECT $x, name FROM " + vertexClassName + " let $x = out(\"" + edgeClassName + "\")";
    final ResultSet resultSet = database.query("sql", queryString);
    int counter = 0;
    while (resultSet.hasNext()) {
      final Result result = resultSet.next();
      final Iterable edge = result.getProperty("$x");
      final Iterator<Identifiable> iter = edge.iterator();
      while (iter.hasNext()) {
        final MutableVertex tMutableVertex = (MutableVertex) database.lookupByRID(iter.next().getIdentity(), true);
        if (doc2Id.equals(tMutableVertex.getIdentity())) {
          ++counter;
        }
      }
    }
    assertThat(counter).isEqualTo(1);
    resultSet.close();
  }

  @Test
  void unwind1() {
    final String className = "testUnwind1";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("i", i);
      doc.set("iSeq", new int[] { i, 2 * i, 4 * i });
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select i, iSeq from " + className + " unwind iSeq");
    for (int i = 0; i < 30; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Object>getProperty("i")).isNotNull();
      assertThat(item.<Object>getProperty("iSeq")).isNotNull();
      final Integer first = item.getProperty("i");
      final Integer second = item.getProperty("iSeq");
      assertThat(first + second == 0 || second % first == 0).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void unwind2() {
    final String className = "testUnwind2";
    database.getSchema().createDocumentType(className);

    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("i", i);
      final List<Integer> iSeq = new ArrayList<>();
      iSeq.add(i);
      iSeq.add(i * 2);
      iSeq.add(i * 4);
      doc.set("iSeq", iSeq);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select i, iSeq from " + className + " unwind iSeq");
    for (int i = 0; i < 30; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<Object>getProperty("i")).isNotNull();
      assertThat(item.<Object>getProperty("iSeq")).isNotNull();
      final Integer first = item.getProperty("i");
      final Integer second = item.getProperty("iSeq");
      assertThat(first + second == 0 || second % first == 0).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubclassIndexes1() {
    final String parent = "testFetchFromSubclassIndexes1_parent";
    final String child1 = "testFetchFromSubclassIndexes1_child1";
    final String child2 = "testFetchFromSubclassIndexes1_child2";
    database.begin();
    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
    database.command("sql", "create document type " + child1 + " extends " + parent);
    database.command("sql", "create document type " + child2 + " extends " + parent);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);

//        DocumentType childClass1 = database.getSchema().createDocumentType(child1);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2);

    parentClass.createProperty("name", Type.STRING);

    database.command("sql", "create index on " + child1 + "(name) NOTUNIQUE");
    database.command("sql", "create index on " + child2 + "(name) NOTUNIQUE");
//        childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
//        childClass2.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().getFirst() instanceof ParallelExecStep).isTrue();
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubclassIndexes2() {
    final String parent = "testFetchFromSubclassIndexes2_parent";
    final String child1 = "testFetchFromSubclassIndexes2_child1";
    final String child2 = "testFetchFromSubclassIndexes2_child2";
    database.begin();
    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);
    final DocumentType childClass1 = database.getSchema().createDocumentType(child1);
    childClass1.addSuperType(parentClass);
    final DocumentType childClass2 = database.getSchema().createDocumentType(child2);
    childClass2.addSuperType(parentClass);

    parentClass.createProperty("name", Type.STRING);
    childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    childClass2.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1' and surname = 'surname1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().getFirst() instanceof ParallelExecStep).isTrue();
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubclassIndexes3() {
    final String parent = "testFetchFromSubclassIndexes3_parent";
    final String child1 = "testFetchFromSubclassIndexes3_child1";
    final String child2 = "testFetchFromSubclassIndexes3_child2";
    database.begin();
    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);
    final DocumentType childClass1 = database.getSchema().createDocumentType(child1);
    childClass1.addSuperType(parentClass);
    final DocumentType childClass2 = database.getSchema().createDocumentType(child2);
    childClass2.addSuperType(parentClass);

    parentClass.createProperty("name", Type.STRING);
    childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1' and surname = 'surname1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().getFirst() instanceof FetchFromTypeExecutionStep).isTrue(); // no index used
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubclassIndexes4() {
    final String parent = "testFetchFromSubclassIndexes4_parent";
    final String child1 = "testFetchFromSubclassIndexes4_child1";
    final String child2 = "testFetchFromSubclassIndexes4_child2";
    database.begin();
    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);
    final DocumentType childClass1 = database.getSchema().createDocumentType(child1);
    childClass1.addSuperType(parent);
    final DocumentType childClass2 = database.getSchema().createDocumentType(child2);
    childClass2.addSuperType(parent);

    parentClass.createProperty("name", Type.STRING);
    childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    childClass2.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    final MutableDocument parentdoc = database.newDocument(parent);
    parentdoc.set("name", "foo");
    parentdoc.save();

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1' and surname = 'surname1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(
        plan.getSteps().getFirst() instanceof FetchFromTypeExecutionStep).isTrue(); // no index, because the superclass is not empty
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubSubclassIndexes() {
    final String parent = "testFetchFromSubSubclassIndexes_parent";
    final String child1 = "testFetchFromSubSubclassIndexes_child1";
    final String child2 = "testFetchFromSubSubclassIndexes_child2";
    final String child2_1 = "testFetchFromSubSubclassIndexes_child2_1";
    final String child2_2 = "testFetchFromSubSubclassIndexes_child2_2";
    database.begin();
    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
    parentClass.createProperty("name", Type.STRING);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);
//        DocumentType childClass2_1 = database.getSchema().createDocumentType(child2_1, childClass2);
//        DocumentType childClass2_2 = database.getSchema().createDocumentType(child2_2, childClass2);
    final DocumentType childClass1 = database.getSchema().createDocumentType(child1);
    childClass1.addSuperType(parent);
    final DocumentType childClass2 = database.getSchema().createDocumentType(child2);
    childClass2.addSuperType(parent);
    final DocumentType childClass2_1 = database.getSchema().createDocumentType(child2_1);
    childClass2_1.addSuperType(child2);
    final DocumentType childClass2_2 = database.getSchema().createDocumentType(child2_2);
    childClass2_2.addSuperType(child2);

    childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    childClass2_1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    childClass2_2.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2_1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2_2);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1' and surname = 'surname1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().getFirst() instanceof ParallelExecStep).isTrue();
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fetchFromSubSubclassIndexesWithDiamond() {
    final String parent = "testFetchFromSubSubclassIndexesWithDiamond_parent";
    final String child1 = "testFetchFromSubSubclassIndexesWithDiamond_child1";
    final String child2 = "testFetchFromSubSubclassIndexesWithDiamond_child2";
    final String child12 = "testFetchFromSubSubclassIndexesWithDiamond_child12";
    database.begin();

    final DocumentType parentClass = database.getSchema().createDocumentType(parent);
//        DocumentType childClass1 = database.getSchema().createDocumentType(child1, parentClass);
//        DocumentType childClass2 = database.getSchema().createDocumentType(child2, parentClass);
//        DocumentType childClass12 =
//                database.getSchema().createDocumentType(child12, childClass1, childClass2);
    final DocumentType childClass1 = database.getSchema().createDocumentType(child1);
    childClass1.addSuperType(parentClass);
    final DocumentType childClass2 = database.getSchema().createDocumentType(child2);
    childClass2.addSuperType(parentClass);
    final DocumentType childClass12 = database.getSchema().createDocumentType(child12);
    childClass12.addSuperType(childClass1);
    childClass12.addSuperType(childClass2);

    parentClass.createProperty("name", Type.STRING);
    childClass1.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    childClass2.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child1);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child2);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(child12);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + parent + " where name = 'name1' and surname = 'surname1'");
    final InternalExecutionPlan plan = (InternalExecutionPlan) result.getExecutionPlan().get();
    assertThat(plan.getSteps().getFirst() instanceof FetchFromTypeExecutionStep).isTrue();
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void indexPlusSort1() {
    final String className = "testIndexPlusSort1";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' order by surname ASC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();

      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(surname.compareTo(lastSurname) > 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort2() {
    final String className = "testIndexPlusSort2";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql",

        "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' order by surname DESC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();

      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(surname.compareTo(lastSurname) < 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort3() {
    final String className = "testIndexPlusSort3";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where name = 'name1' order by name DESC, surname DESC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();

      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(((String) item.<String>getProperty("surname")).compareTo(lastSurname) < 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort4() {
    final String className = "testIndexPlusSort4";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where name = 'name1' order by name ASC, surname ASC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();

      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(surname.compareTo(lastSurname) > 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort5() {
    final String className = "testIndexPlusSort5";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createProperty("address", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname, address) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.set("address", "address" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' order by surname ASC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(surname.compareTo(lastSurname) > 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort6() {
    final String className = "testIndexPlusSort6";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createProperty("address", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname, address) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.set("address", "address" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' order by surname DESC");
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
      final String surname = item.<String>getProperty("surname");
      if (i > 0) {
        assertThat(surname.compareTo(lastSurname) < 0).isTrue();
      }
      lastSurname = surname;
    }
    assertThat(result.hasNext()).isFalse();
    final ExecutionPlan plan = result.getExecutionPlan().get();
    assertThat(plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count()).isEqualTo(1);
    assertThat(plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count()).isEqualTo(0);
    result.close();
  }

  @Test
  void indexPlusSort7() {
    final String className = "testIndexPlusSort7";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    clazz.createProperty("address", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname, address) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.set("address", "address" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name = 'name1' order by address DESC");

    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isTrue();
    result.close();
  }

  @Test
  void indexPlusSort8() {
    final String className = "testIndexPlusSort8";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql",
        "select from " + className + " where name = 'name1' order by name ASC, surname DESC");
    for (int i = 0; i < 3; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isTrue();
    result.close();
  }

  @Test
  void indexPlusSort9() {
    final String className = "testIndexPlusSort9";
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    database.begin();
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " order by name , surname ASC");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isFalse();
    result.close();
  }

  @Test
  void indexPlusSort10() {
    final String className = "testIndexPlusSort10";
    database.begin();
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " order by name desc, surname desc");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isFalse();
    result.close();
  }

  @Test
  void indexPlusSort11() {
    final String className = "testIndexPlusSort11";
    database.begin();
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " order by name asc, surname desc");
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("surname")).isNotNull();
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isTrue();
    result.close();
  }

  @Test
  void indexPlusSort12() {
    final String className = "testIndexPlusSort12";
    database.begin();
    final DocumentType clazz = database.getSchema().createDocumentType(className);
    clazz.createProperty("name", Type.STRING);
    clazz.createProperty("surname", Type.STRING);
    database.command("sql", "create index on " + className + " (name, surname) NOTUNIQUE");

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i % 3);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " order by name");
    String last = null;
    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isNotNull();
      final String name = item.getProperty("name");
      //System.out.println(name);
      if (i > 0) {
        assertThat(name.compareTo(last) >= 0).isTrue();
      }
      last = name;
    }
    assertThat(result.hasNext()).isFalse();
    assertThat(result.hasNext()).isFalse();
    boolean orderStepFound = false;
    for (final ExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    assertThat(orderStepFound).isFalse();
    result.close();
  }

  @Test
  void selectFromStringParam() {
    final String className = "testSelectFromStringParam";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from ?", className);

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void selectFromStringNamedParam() {
    final String className = "testSelectFromStringNamedParam";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();
    final Map<String, Object> params = new HashMap<>();
    params.put("target", className);
    final ResultSet result = database.query("sql", "select from :target", params);

    for (int i = 0; i < 10; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(("" + item.getProperty("name")).startsWith("name")).isTrue();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void matches() {
    final String className = "testMatches";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " where name matches 'name1'");

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("name1");
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void range() {
    final String className = "testRange";
    database.getSchema().createDocumentType(className);

    database.begin();
    final MutableDocument doc = database.newDocument(className);
    doc.set("name", new String[] { "a", "b", "c", "d" });
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select name[0..3] as names from " + className);

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object names = item.getProperty("names");
      if (names == null) {
        fail("");
      }
      if (names instanceof Collection collection) {
        assertThat(collection.size()).isEqualTo(3);
        final Iterator iter = collection.iterator();
        assertThat(iter.next()).isEqualTo("a");
        assertThat(iter.next()).isEqualTo("b");
        assertThat(iter.next()).isEqualTo("c");
      } else if (names.getClass().isArray()) {
        assertThat(Array.getLength(names)).isEqualTo(3);
      } else {
        fail("");
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void rangeParams1() {
    final String className = "testRangeParams1";
    database.getSchema().createDocumentType(className);
    database.begin();

    final MutableDocument doc = database.newDocument(className);
    doc.set("name", new String[] { "a", "b", "c", "d" });
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select name[?..?] as names from " + className, 0, 3);

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object names = item.getProperty("names");
      if (names == null) {
        fail("");
      }
      if (names instanceof Collection collection) {
        assertThat(collection.size()).isEqualTo(3);
        final Iterator iter = collection.iterator();
        assertThat(iter.next()).isEqualTo("a");
        assertThat(iter.next()).isEqualTo("b");
        assertThat(iter.next()).isEqualTo("c");
      } else if (names.getClass().isArray()) {
        assertThat(Array.getLength(names)).isEqualTo(3);
      } else {
        fail("");
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void rangeParams2() {
    final String className = "testRangeParams2";
    database.getSchema().createDocumentType(className);
    database.begin();

    final MutableDocument doc = database.newDocument(className);
    doc.set("name", new String[] { "a", "b", "c", "d" });
    doc.save();
    database.commit();

    final Map<String, Object> params = new HashMap<>();
    params.put("a", 0);
    params.put("b", 3);
    final ResultSet result = database.query("sql", "select name[:a..:b] as names from " + className, params);

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object names = item.getProperty("names");
      if (names == null) {
        fail("");
      }
      if (names instanceof Collection collection) {
        assertThat(collection.size()).isEqualTo(3);
        final Iterator iter = collection.iterator();
        assertThat(iter.next()).isEqualTo("a");
        assertThat(iter.next()).isEqualTo("b");
        assertThat(iter.next()).isEqualTo("c");
      } else if (names.getClass().isArray()) {
        assertThat(Array.getLength(names)).isEqualTo(3);
      } else {
        fail("");
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void ellipsis() {
    final String className = "testEllipsis";
    database.getSchema().createDocumentType(className);
    database.begin();

    final MutableDocument doc = database.newDocument(className);
    doc.set("name", new String[] { "a", "b", "c", "d" });
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select name[0...2] as names from " + className);

    for (int i = 0; i < 1; i++) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      final Object names = item.getProperty("names");
      if (names == null) {
        fail("");
      }
      if (names instanceof Collection collection) {
        assertThat(collection.size()).isEqualTo(3);
        final Iterator iter = collection.iterator();
        assertThat(iter.next()).isEqualTo("a");
        assertThat(iter.next()).isEqualTo("b");
        assertThat(iter.next()).isEqualTo("c");
      } else if (names.getClass().isArray()) {
        assertThat(Array.getLength(names)).isEqualTo(3);
        assertThat(Array.get(names, 0)).isEqualTo("a");
        assertThat(Array.get(names, 1)).isEqualTo("b");
        assertThat(Array.get(names, 2)).isEqualTo("c");
      } else {
        fail("");
      }
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void newRid() {
    final ResultSet result = database.query("sql", "select {\"@rid\":\"#12:0\"} as theRid ");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final Object rid = item.getProperty("theRid");
    assertThat(rid instanceof Identifiable).isTrue();
    final Identifiable id = (Identifiable) rid;
    assertThat(id.getIdentity().getBucketId()).isEqualTo(12);
    assertThat(id.getIdentity().getPosition()).isEqualTo(0L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void nestedProjections1() {
    final String className = "testNestedProjections1";
    database.command("sql", "create document type " + className).close();
    database.begin();
    final MutableDocument elem1 = database.newDocument(className);
    elem1.set("name", "a");
    elem1.save();

    final MutableDocument elem2 = database.newDocument(className);
    elem2.set("name", "b");
    elem2.set("surname", "lkj");
    elem2.save();

    final MutableDocument elem3 = database.newDocument(className);
    elem3.set("name", "c");
    elem3.save();

    final MutableDocument elem4 = database.newDocument(className);
    elem4.set("name", "d");
    elem4.set("elem1", elem1);
    elem4.set("elem2", elem2);
    elem4.set("elem3", elem3);
    elem4.save();

    database.commit();
    final ResultSet result = database.query("sql",
        "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();

    final Result elem1Result = item.getProperty("elem1");
    assertThat(elem1Result.<String>getProperty("name")).isEqualTo("a");
    assertThat(elem1Result.<RID>getProperty(RID_PROPERTY)).isEqualTo(elem1.getIdentity());
    assertThat(elem1Result.<String>getProperty(TYPE_PROPERTY)).isEqualTo(elem1.getTypeName());

    final Result elem2Result = item.getProperty("elem2");
    assertThat(elem2Result.<String>getProperty("name")).isEqualTo("b");
    assertThat(elem2Result.<String>getProperty("surname")).isNull();
    assertThat(elem2Result.<RID>getProperty(RID_PROPERTY)).isEqualTo(elem2.getIdentity());
    assertThat(elem2Result.<String>getProperty(TYPE_PROPERTY)).isEqualTo(elem2.getTypeName());

    result.close();
  }

  @Test
  void simpleCollectionFiltering() {
    final String className = "testSimpleCollectionFiltering";
    database.command("sql", "create document type " + className).close();
    database.begin();
    final MutableDocument elem1 = database.newDocument(className);
    final List<String> coll = new ArrayList<>();
    coll.add("foo");
    coll.add("bar");
    coll.add("baz");
    elem1.set("coll", coll);
    elem1.save();
    database.commit();

    ResultSet result = database.query("sql", "select coll[='foo'] as filtered from " + className);
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    List res = item.getProperty("filtered");
    assertThat(res.size()).isEqualTo(1);
    assertThat(res.getFirst()).isEqualTo("foo");
    result.close();

    result = database.query("sql", "select coll[<'ccc'] as filtered from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    res = item.getProperty("filtered");
    assertThat(res.size()).isEqualTo(2);
    result.close();

    result = database.query("sql", "select coll[LIKE 'ba%'] as filtered from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    res = item.getProperty("filtered");
    assertThat(res.size()).isEqualTo(2);
    result.close();

    result = database.query("sql", "select coll[in ['bar']] as filtered from " + className);
    assertThat(result.hasNext()).isTrue();
    item = result.next();
    res = item.getProperty("filtered");
    assertThat(res.size()).isEqualTo(1);
    assertThat(res.getFirst()).isEqualTo("bar");
    result.close();
  }

  @Test
  void contaninsWithConversion() {
    final String className = "testContaninsWithConversion";
    database.command("sql", "create document type " + className).close();
    database.begin();
    final MutableDocument elem1 = database.newDocument(className);
    List<Long> coll = new ArrayList<>();
    coll.add(1L);
    coll.add(3L);
    coll.add(5L);
    elem1.set("coll", coll);
    elem1.save();

    final MutableDocument elem2 = database.newDocument(className);
    coll = new ArrayList<>();
    coll.add(2L);
    coll.add(4L);
    coll.add(6L);
    elem2.set("coll", coll);
    elem2.save();

    database.commit();
    ResultSet result = database.query("sql", "select from " + className + " where coll contains 1");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "select from " + className + " where coll contains 1L");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    result = database.query("sql", "select from " + className + " where coll contains 12L");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void containsIntegers() {
    final String className = "testContains";

    final DocumentType clazz1 = database.getSchema().createDocumentType(className);
    clazz1.createProperty("list", Type.LIST);

    database.getSchema().createDocumentType("embeddedList");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument document = database.newDocument(className);
        document.set("list", new ArrayList<>());

        for (int j = i; j < i + 3; j++)
          document.newEmbeddedDocument("embeddedList", "list").set("value", j);

        document.save();
      }
    });

    int totalFound = 0;
    for (final ResultSet result = database.query("sql",
        "select from " + className + " where list contains ( value = 3 )"); result.hasNext(); ) {
      final Result item = result.next();
      final List<EmbeddedDocument> embeddedList = item.getProperty("list");

      final List<Integer> valueMatches = new ArrayList<>();
      for (final EmbeddedDocument d : embeddedList)
        valueMatches.add(d.getInteger("value"));

      assertThat(valueMatches.contains(3)).isTrue();

      ++totalFound;
    }

    assertThat(totalFound).isEqualTo(3);
  }

  @Test
  void containsStrings() {
    final String className = "testContains";

    final DocumentType clazz1 = database.getSchema().createDocumentType(className);
    clazz1.createProperty("list", Type.LIST);

    database.getSchema().createDocumentType("embeddedList");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument document = database.newDocument(className);
        document.set("list", new ArrayList<>());

        for (int j = i; j < i + 3; j++)
          document.newEmbeddedDocument("embeddedList", "list").set("value", "" + j);

        document.save();
      }
    });

    int totalFound = 0;
    for (final ResultSet result = database.query("sql",
        "select from " + className + " where list contains ( value = '3' )"); result.hasNext(); ) {
      final Result item = result.next();
      final List<EmbeddedDocument> embeddedList = item.getProperty("list");

      final List<String> valueMatches = new ArrayList<>();
      for (final EmbeddedDocument d : embeddedList)
        valueMatches.add(d.getString("value"));

      assertThat(valueMatches.contains("3")).isTrue();

      ++totalFound;
    }

    assertThat(totalFound).isEqualTo(3);
  }

  @Test
  void containsStringsInMap() {
    final String className = "testContains";

    final DocumentType clazz1 = database.getSchema().createDocumentType(className);
    clazz1.createProperty("list", Type.LIST);

    database.getSchema().createDocumentType("embeddedList");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument document = database.newDocument(className);

        final List<Map> list = new ArrayList<>();
        document.set("list", list);

        for (int j = i; j < i + 3; j++)
          list.add(Map.of("value", "" + j));

        document.save();
      }
    });

    int totalFound = 0;
    for (final ResultSet result = database.query("sql",
        "select from " + className + " where list contains ( value = '3' )"); result.hasNext(); ) {
      final Result item = result.next();
      final List<Map> embeddedList = item.getProperty("list");

      final List<String> valueMatches = new ArrayList<>();
      for (final Map d : embeddedList)
        valueMatches.add((String) d.get("value"));

      assertThat(valueMatches.contains("3")).isTrue();

      ++totalFound;
    }

    assertThat(totalFound).isEqualTo(3);
  }

  @Test
  void indexPrefixUsage() {
    // issue #7636
    final String className = "testIndexPrefixUsage";
    database.begin();
    database.command("sql", "create document type " + className).close();
    database.command("sql", "create property " + className + ".id LONG").close();
    database.command("sql", "create property " + className + ".name STRING").close();
    database.command("sql", "create index on " + className + "(id, name) UNIQUE").close();
    database.command("sql", "insert into " + className + " set id = 1 , name = 'Bar'").close();

    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " where name = 'Bar'");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void namedParams() {
    final String className = "testNamedParams";
    database.begin();
    database.command("sql", "create document type " + className).close();
    database.command("sql", "insert into " + className + " set name = 'Foo', surname = 'Fox'").close();
    database.command("sql", "insert into " + className + " set name = 'Bar', surname = 'Bax'").close();

    database.commit();
    final Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    params.put("p2", "Fox");
    final ResultSet result = database.query("sql", "select from " + className + " where name = :p1 and surname = :p2", params);
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void namedParamsWithIndex() {
    final String className = "testNamedParamsWithIndex";
    database.command("sql", "create document type " + className).close();
    database.begin();
    database.command("sql", "create property " + className + ".name STRING").close();
    database.command("sql", "create index ON " + className + " (name) NOTUNIQUE").close();
    database.command("sql", "insert into " + className + " set name = 'Foo'").close();
    database.command("sql", "insert into " + className + " set name = 'Bar'").close();
    database.commit();

    final Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    final ResultSet result = database.query("sql", "select from " + className + " where name = :p1", params);
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void isDefined() {
    final String className = "testIsDefined";
    database.command("sql", "create document type " + className).close();
    database.begin();
    database.command("sql", "insert into " + className + " set name = 'Foo'").close();
    database.command("sql", "insert into " + className + " set sur = 'Bar'").close();
    database.command("sql", "insert into " + className + " set sur = 'Barz'").close();
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name is defined");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void isNotDefined() {
    final String className = "testIsNotDefined";
    database.command("sql", "create document type " + className).close();
    database.begin();
    database.command("sql", "insert into " + className + " set name = 'Foo'").close();
    database.command("sql", "insert into " + className + " set name = null, sur = 'Bar'").close();
    database.command("sql", "insert into " + className + " set sur = 'Barz'").close();
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where name is not defined");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void ridPagination1() {
    final String className = "testRidPagination1";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
    final int[] clusterIds = new int[clazz.getBuckets(false).size()];
    if (clusterIds.length < 3) {
      return;
    }
    System.arraycopy(clazz.getBuckets(false).stream().mapToInt(x -> x.getFileId()).toArray(), 0, clusterIds, 0, clusterIds.length);
    Arrays.sort(clusterIds);

    for (int i = 0; i < clusterIds.length; i++) {
      final MutableDocument elem = database.newDocument(className);
      elem.set("cid", clusterIds[i]);
      elem.save(database.getSchema().getBucketById(clusterIds[i]).getName());
    }
    database.commit();

    final ResultSet result = database.query("sql", "select from " + className + " where @rid >= #" + clusterIds[1] + ":0");
    final ExecutionPlan execPlan = result.getExecutionPlan().get();
    for (final ExecutionStep ExecutionStep : execPlan.getSteps()) {
      if (ExecutionStep instanceof FetchFromTypeExecutionStep) {
        assertThat(ExecutionStep.getSubSteps().size()).isEqualTo(clusterIds.length - 1);
        // clusters - 1 + fetch from tx...
      }
    }
    int count = 0;
    while (result.hasNext()) {
      count++;
      result.next();
    }
    result.close();
    assertThat(count).isEqualTo(clusterIds.length - 1);
  }

  @Test
  void ridPagination2() {
    final String className = "testRidPagination2";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
    final int[] clusterIds = new int[clazz.getBuckets(false).size()];
    if (clusterIds.length < 3) {
      return;
    }
    System.arraycopy(clazz.getBuckets(false).stream().mapToInt(x -> x.getFileId()).toArray(), 0, clusterIds, 0, clusterIds.length);
    Arrays.sort(clusterIds);

    for (int i = 0; i < clusterIds.length; i++) {
      final MutableDocument elem = database.newDocument(className);
      elem.set("cid", clusterIds[i]);
      elem.save(database.getSchema().getBucketById(clusterIds[i]).getName());
    }
    database.commit();

    final Map<String, Object> params = new HashMap<>();
    params.put("rid", new RID(database, clusterIds[1], 0));
    final ResultSet result = database.query("sql", "select from " + className + " where @rid >= :rid", params);
    final ExecutionPlan execPlan = result.getExecutionPlan().get();
    for (final ExecutionStep ExecutionStep : execPlan.getSteps()) {
      if (ExecutionStep instanceof FetchFromTypeExecutionStep) {
        assertThat(ExecutionStep.getSubSteps().size()).isEqualTo(clusterIds.length - 1);
        // clusters - 1 + fetch from tx...
      }
    }
    int count = 0;
    while (result.hasNext()) {
      count++;
      result.next();
    }
    result.close();
    assertThat(count).isEqualTo(clusterIds.length - 1);
  }

  @Test
  void containsWithSubquery() {
    final String className = "testContainsWithSubquery";
    database.begin();
    final DocumentType clazz1 = database.getSchema().getOrCreateDocumentType(className + 1);
    final DocumentType clazz2 = database.getSchema().getOrCreateDocumentType(className + 2);
    clazz2.createProperty("tags", Type.LIST);

    database.command("sql", "insert into " + className + 1 + "  set name = 'foo'");

    database.command("sql", "insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    database.command("sql", "insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    database.command("sql", "insert into " + className + 2 + "  set tags = ['foo']");
    database.commit();

    try (final ResultSet result = database.query("sql",
        "select from " + className + 2 + " where tags contains (select from " + className + 1 + " where name = 'foo')")) {

      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void inWithSubquery() {
    final String className = "testInWithSubquery";
    database.begin();
    final DocumentType clazz1 = database.getSchema().getOrCreateDocumentType(className + 1);
    final DocumentType clazz2 = database.getSchema().getOrCreateDocumentType(className + 2);
    clazz2.createProperty("tags", Type.LIST);

    database.command("sql", "insert into " + className + 1 + "  set name = 'foo'");

    database.command("sql", "insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    database.command("sql", "insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    database.command("sql", "insert into " + className + 2 + "  set tags = ['foo']");
    database.commit();

    try (final ResultSet result = database.query("sql",
        "select from " + className + 2 + " where (select from " + className + 1 + " where name = 'foo') in tags")) {

      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void containsAny() {
    final String className = "testContainsAny";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
//        clazz.createProperty("tags", Type.LIST, Type.STRING);
    clazz.createProperty("tags", Type.LIST);

    database.command("sql", "insert into " + className + "  set tags = ['foo', 'bar']");
    database.command("sql", "insert into " + className + "  set tags = ['bbb', 'FFF']");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','baz']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','bar']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','bbb']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['xx','baz']")) {
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany []")) {
      assertThat(result.hasNext()).isFalse();
    }
  }

  //    @Test   TODO
  public void testContainsAnyWithIndex() {
    final String className = "testContainsAnyWithIndex";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
//        Property prop = clazz.createProperty("tags", Type.LIST, Type.STRING);
    final Property prop = clazz.createProperty("tags", Type.LIST);
    prop.createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    database.command("sql", "insert into " + className + "  set tags = ['foo', 'bar']");
    database.command("sql", "insert into " + className + "  set tags = ['bbb', 'FFF']");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','baz']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','bar']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['foo','bbb']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany ['xx','baz']")) {
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsany []")) {
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }
  }

  @Test
  void containsAll() {
    final String className = "testContainsAll";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    clazz.createProperty("tags", Type.LIST);

    database.begin();
    database.command("sql", "insert into " + className + "  set tags = ['foo', 'bar']");
    database.command("sql", "insert into " + className + "  set tags = ['foo', 'FFF']");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsall ['foo','bar']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tags containsall ['foo']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void between() {
    final String className = "testBetween";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();

    database.command("sql", "insert into " + className + "  set name = 'foo1', val = 1");
    database.command("sql", "insert into " + className + "  set name = 'foo2', val = 2");
    database.command("sql", "insert into " + className + "  set name = 'foo3', val = 3");
    database.command("sql", "insert into " + className + "  set name = 'foo4', val = 4");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where val between 2 and 3")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void inWithIndex() {
    final String className = "testInWithIndex";
    database.begin();
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    final Property prop = clazz.createProperty("tag", Type.STRING);
    prop.createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    database.command("sql", "insert into " + className + "  set tag = 'foo'");
    database.command("sql", "insert into " + className + "  set tag = 'bar'");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in ['foo','baz']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in ['foo','bar']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in []")) {
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }

    final List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in (?)", params)) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isTrue();
    }
  }

  @Test
  void inWithoutIndex() {
    final String className = "testInWithoutIndex";
    database.begin();
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    final Property prop = clazz.createProperty("tag", Type.STRING);

    database.command("sql", "insert into " + className + "  set tag = 'foo'");
    database.command("sql", "insert into " + className + "  set tag = 'bar'");
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in ['foo','baz']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in ['foo','bar']")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in []")) {
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isFalse();
    }

    final List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    try (final ResultSet result = database.query("sql", "select from " + className + " where tag in (?)", params)) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      assertThat(result.getExecutionPlan().get().getSteps().stream().anyMatch(x -> x instanceof FetchFromIndexStep)).isFalse();
    }
  }

  //    @Test
  public void testListOfMapsContains() {
    final String className = "testListOfMapsContains";

    final DocumentType clazz1 = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
    final Property prop = clazz1.createProperty("thelist", Type.LIST);

    database.command("sql", "insert INTO " + className + " SET thelist = [{name:\"Jack\"}]").close();
    database.command("sql", "insert INTO " + className + " SET thelist = [{name:\"Joe\"}]").close();
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where thelist CONTAINS ( name = ?)",
        "Jack")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void containsMultipleConditions() {
    final String className = "testContainsMultipleConditions";

    database.getSchema().getOrCreateVertexType("Legajo");
    database.getSchema().getOrCreateVertexType("Interviniente");
    database.getSchema().getOrCreateVertexType("PersonaDifusa");
    database.getSchema().getOrCreateVertexType("PresuntoResponsable");

    database.getSchema().getOrCreateEdgeType("Legajo_intervinientes");
    database.getSchema().getOrCreateEdgeType("Interviniente_roles");
    database.getSchema().getOrCreateEdgeType("Interviniente_persona");

    database.begin();
    MutableVertex legajo = database.newVertex("Legajo").set("cuij", "21087591856").save();
    database.newVertex("Legajo").set("cuij", "1").save();
    database.newVertex("Legajo").set("cuij", "2").save();

    MutableVertex interviniente = database.newVertex("Interviniente").set("id", 0).save();
    database.newVertex("Interviniente").set("id", 1).save();
    database.newVertex("Interviniente").set("id", 2).save();

    MutableVertex personaDifusa = database.newVertex("PersonaDifusa").set("id", 0).set("nroDoc", "1234567890").save();
    database.newVertex("PersonaDifusa").set("id", 1).save();
    database.newVertex("PersonaDifusa").set("id", 2).save();

    MutableVertex presuntoResponsable = database.newVertex("PresuntoResponsable").set("id", 0).save();

    legajo.newEdge("Legajo_intervinientes", interviniente).save();

    interviniente.newEdge("Interviniente_roles", presuntoResponsable).save();
    interviniente.newEdge("Interviniente_persona", personaDifusa).save();

    database.commit();

    final String TEST_QUERY = "select cuij, count(*) as count from Legajo \n" +//
        "let intervinientes = out('Legajo_intervinientes')" + //
        "where cuij = '21087591856' and " + //
        "$intervinientes.out('Interviniente_persona') contains (nroDoc.length() > 5) and " + //
        "      $intervinientes.out('Interviniente_roles') contains( @this instanceof 'PresuntoResponsable' )";

    try (final ResultSet result = database.query("sql", TEST_QUERY)) {
      final Result row = result.nextIfAvailable();
      assertThat(row.<String>getProperty("cuij")).isEqualTo("21087591856");
      assertThat((Long) row.getProperty("count")).isEqualTo(1L);
    }
  }

  @Test
  void containsEmptyCollection() {
    final String className = "testContainsEmptyCollection";
    database.begin();

    database.getSchema().getOrCreateDocumentType(className);

    database.command("sql", "insert INTO " + className + " content {\"name\": \"jack\", \"age\": 22}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();

    database.commit();
    try (final ResultSet result = database.query("sql", "select from " + className + " where test contains []")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void containsCollection() {
    final String className = "testContainsCollection";

    database.getSchema().getOrCreateDocumentType(className);
    database.begin();

    database.command("sql", "insert INTO " + className + " content {\"name\": \"jack\", \"age\": 22}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}").close();
    database.command("sql", "INSERT INTO " + className + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();
    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where test contains [1]")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void heapLimitForOrderBy() {
    final Long oldValue = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3);

      final String className = "testHeapLimitForOrderBy";

      database.getSchema().getOrCreateDocumentType(className);
      database.begin();

      database.command("sql", "insert INTO " + className + " set name = 'a'").close();
      database.command("sql", "insert INTO " + className + " set name = 'b'").close();
      database.command("sql", "insert INTO " + className + " set name = 'c'").close();
      database.command("sql", "insert INTO " + className + " set name = 'd'").close();
      database.commit();

      assertThatThrownBy(() -> {
            try (final ResultSet result = database.query("sql", "select from " + className + " ORDER BY name")) {
              result.forEachRemaining(x -> x.getProperty("name"));
            }
          }
      ).isInstanceOf(CommandExecutionException.class);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  void xor() {
    try (final ResultSet result = database.query("sql", "select 15 ^ 4 as foo")) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat((int) item.getProperty("foo")).isEqualTo(11);
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void like() {
    final String className = "testLike";

    database.getSchema().getOrCreateDocumentType(className);
    database.begin();

    database.command("sql", "insert INTO " + className + " content {\"name\": \"foobarbaz\"}").close();
    database.command("sql", "insert INTO " + className + " content {\"name\": \"test[]{}()|*^.test\"}").close();

    database.commit();

    try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE 'foo%'")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
    try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE '%foo%baz%'")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }
    try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE '%bar%'")) {
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE 'bar%'")) {
      assertThat(result.hasNext()).isFalse();
    }

    try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE '%bar'")) {
      assertThat(result.hasNext()).isFalse();
    }

    final String specialChars = "[]{}()|*^.";
    for (final char c : specialChars.toCharArray()) {
      try (final ResultSet result = database.query("sql", "select from " + className + " where name LIKE '%" + c + "%'")) {
        assertThat(result.hasNext()).isTrue();
        result.next();
        assertThat(result.hasNext()).isFalse();
      }
    }
  }

  @Test
  void countGroupBy() {
    // issue #9288
    final String className = "testCountGroupBy";
    database.getSchema().createDocumentType(className);
    database.begin();
    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("type", i % 2 == 0 ? "even" : "odd");
      doc.set("val", i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select count(val) as count from " + className + " limit 3");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat((long) item.getProperty("count")).isEqualTo(10L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

//    @Test
//    public void testTimeout() {
//        String className = "testTimeout";
//        final String functionName = getClass().getSimpleName() + "_sleep";
//        database.getSchema().createDocumentType(className);
//
//        ((SQLQueryEngine) ctx.getDatabase().getQueryEngine("sql"))
//                .registerFunction(
//                        functionName,
//                        new SQLFunction() {
//
//                            @Override
//                            public Object execute(
//                                    Object self,
//                                    Identifiable currentRecord,
//                                    Object currentResult,
//                                    Object[] params,
//                                    CommandContext context) {
//                                try {
//                                    Thread.sleep(5);
//                                } catch (InterruptedException e) {
//                                }
//                                return null;
//                            }
//
//                            @Override
//                            public void config(Object[] configuredParameters) {}
//
//                            @Override
//                            public boolean aggregateResults() {
//                                return false;
//                            }
//
//                            @Override
//                            public boolean filterResult() {
//                                return false;
//                            }
//
//                            @Override
//                            public String getName() {
//                                return functionName;
//                            }
//
//                            @Override
//                            public int getMinParams() {
//                                return 0;
//                            }
//
//                            @Override
//                            public int getMaxParams() {
//                                return 0;
//                            }
//
//                            @Override
//                            public String getSyntax() {
//                                return "";
//                            }
//
//                            @Override
//                            public Object getResult() {
//                                return null;
//                            }
//
//                            @Override
//                            public void setResult(Object iResult) {}
//
//                            @Override
//                            public boolean shouldMergeDistributedResult() {
//                                return false;
//                            }
//
//                            @Override
//                            public Object mergeDistributedResult(List<Object> resultsToMerge) {
//                                return null;
//                            }
//                        });
//        for (int i = 0; i < 3; i++) {
//            MutableDocument doc = database.newDocument(className);
//            doc.set("type", i % 2 == 0 ? "even" : "odd");
//            doc.set("val", i);
//            doc.save();
//        }
//        try (ResultSet result =
//                     database.query("sql", "select " + functionName + "(), * from " + className + " timeout 1")) {
//            while (result.hasNext()) {
//                result.next();
//            }
//            Assertions.fail();
//        } catch (TimeoutException ex) {
//
//        }
//
//        try (ResultSet result =
//                     database.query("sql", "select " + functionName + "(), * from " + className + " timeout 100")) {
//            while (result.hasNext()) {
//                result.next();
//            }
//        } catch (TimeoutException ex) {
//            Assertions.fail();
//        }
//    }

  @Test
  void simpleRangeQueryWithIndexGTE() {
    final String className = "testSimpleRangeQueryWithIndexGTE";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
    final Property prop = clazz.createProperty("name", Type.STRING);
    prop.createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " WHERE name >= 'name5'");

    for (int i = 0; i < 5; i++) {
      assertThat(result.hasNext()).isTrue();
      result.next();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void simpleRangeQueryWithIndexLTE() {
    final String className = "testSimpleRangeQueryWithIndexLTE";
    final DocumentType clazz = database.getSchema().getOrCreateDocumentType(className);
    database.begin();
    final Property prop = clazz.createProperty("name", Type.STRING);
    prop.createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

    for (int i = 0; i < 10; i++) {
      final MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.save();
    }
    database.commit();
    final ResultSet result = database.query("sql", "select from " + className + " WHERE name <= 'name5'");

    for (int i = 0; i < 6; i++) {
      assertThat(result.hasNext()).isTrue();
      result.next();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  //    @Test
  public void testIndexWithSubquery() {
    final String classNamePrefix = "testIndexWithSubquery_";
    database.begin();
    database.command("sql", "create Vertex Type " + classNamePrefix + "Ownership  abstract;").close();
    database.command("sql", "create vertex type " + classNamePrefix + "User ;").close();
    database.command("sql", "create property " + classNamePrefix + "User.id String;").close();
    database.command("sql", "create index ON " + classNamePrefix + "User(id) unique;").close();
    database.command("sql", "create vertex type " + classNamePrefix + "Report extends " + classNamePrefix + "Ownership;").close();
    database.command("sql", "create property " + classNamePrefix + "Report.id String;").close();
    database.command("sql", "create property " + classNamePrefix + "Report.label String;").close();
    database.command("sql", "create property " + classNamePrefix + "Report.format String;").close();
    database.command("sql", "create property " + classNamePrefix + "Report.source String;").close();
    database.command("sql", "create edge type " + classNamePrefix + "hasOwnership ;").close();
    database.command("sql", "insert into " + classNamePrefix + "User content {id:\"admin\"};");
    database.command("sql", "insert into " + classNamePrefix
        + "Report content {format:\"PDF\", id:\"rep1\", label:\"Report 1\", source:\"Report1.src\"};").close();
    database.command("sql", "insert into " + classNamePrefix
        + "Report content {format:\"CSV\", id:\"rep2\", label:\"Report 2\", source:\"Report2.src\"};").close();
    database.command("sql",
        "create edge " + classNamePrefix + "hasOwnership from (select from " + classNamePrefix + "User) to (select from "
            + classNamePrefix + "Report);").close();

    database.commit();
    try (final ResultSet rs = database.query("sql",
        "select from " + classNamePrefix + "Report where id in (select out('" + classNamePrefix + "hasOwnership').id from "
            + classNamePrefix + "User where id = 'admin');")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
    }

    database.command("sql", "create index ON " + classNamePrefix + "Report(id) unique;").close();

    try (final ResultSet rs = database.query("sql",
        "select from " + classNamePrefix + "Report where id in (select out('" + classNamePrefix + "hasOwnership').id from "
            + classNamePrefix + "User where id = 'admin');")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void exclude() {
    final String className = "TestExclude";
    database.begin();
    database.getSchema().createDocumentType(className);
    final MutableDocument doc = database.newDocument(className);
    doc.set("name", "foo");
    doc.set("surname", "bar");
    doc.save();
    database.commit();

    final ResultSet result = database.query("sql", "select *, !surname from " + className);
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("name")).isEqualTo("foo");
    assertThat(item.<String>getProperty("surname")).isNull();

    result.close();
  }

  @Test
  void orderByLet() {
    final String className = "testOrderByLet";
    database.setAutoTransaction(true);
    database.getSchema().createDocumentType(className);
    MutableDocument doc = database.newDocument(className);
    doc.set("name", "abbb");
    doc.save();

    doc = database.newDocument(className);
    doc.set("name", "baaa");
    doc.save();

    try (final ResultSet result = database.query("sql",
        "select from " + className + " LET $order = name.substring(1) ORDER BY $order ASC LIMIT 1")) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("baaa");
    }
    try (final ResultSet result = database.query("sql",
        "select from " + className + " LET $order = name.substring(1) ORDER BY $order DESC LIMIT 1")) {
      assertThat(result.hasNext()).isTrue();
      final Result item = result.next();
      assertThat(item).isNotNull();
      assertThat(item.<String>getProperty("name")).isEqualTo("abbb");
    }
  }

  @Test
  void schemaMap() {
    database.command("sql", "CREATE DOCUMENT TYPE SchemaMap");
    database.command("sql", "ALTER TYPE SchemaMap CUSTOM label = 'Document'");
    final ResultSet result = database.query("sql", "SELECT map(name,custom.label) as map FROM schema:types");

    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item).isNotNull();

    Object map = item.getProperty("map");
    assertThat(map instanceof Map).isTrue();

    assertThat(((Map<?, ?>) map).get("SchemaMap")).isEqualTo("Document");

    result.close();
  }
}
