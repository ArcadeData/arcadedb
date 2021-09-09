/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

public class OCommandExecutorSQLSelectTestIndex {

  @Test
  public void testIndexSqlEmbeddedList() {

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("memory:embeddedList");
    databaseDocumentTx.create();
    try {

      databaseDocumentTx.command(new OCommandSQL("create class Foo")).execute();
      databaseDocumentTx
          .command(new OCommandSQL("create property Foo.bar EMBEDDEDLIST STRING"))
          .execute();
      databaseDocumentTx
          .command(new OCommandSQL("create index Foo.bar on Foo (bar) NOTUNIQUE"))
          .execute();
      databaseDocumentTx.command(new OCommandSQL("insert into Foo set bar = ['yep']")).execute();
      List<ODocument> results =
          databaseDocumentTx
              .command(new OCommandSQL("select from Foo where bar = 'yep'"))
              .execute();
      assertEquals(results.size(), 1);

      final OIndex index =
          databaseDocumentTx
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex(databaseDocumentTx, "Foo.bar");
      assertEquals(index.getInternal().size(), 1);
    } finally {
      databaseDocumentTx.drop();
    }
  }

  @Test
  public void testIndexOnHierarchyChange() {
    // issue #5743
    ODatabaseDocumentTx databaseDocumentTx =
        new ODatabaseDocumentTx(
            "memory:OCommandExecutorSQLSelectTestIndex_testIndexOnHierarchyChange");
    databaseDocumentTx.create();
    try {

      databaseDocumentTx.command(new OCommandSQL("CREATE CLASS Main ABSTRACT")).execute();
      databaseDocumentTx.command(new OCommandSQL("CREATE PROPERTY Main.uuid String")).execute();
      databaseDocumentTx
          .command(new OCommandSQL("CREATE INDEX Main.uuid UNIQUE_HASH_INDEX"))
          .execute();
      databaseDocumentTx
          .command(new OCommandSQL("CREATE CLASS Base EXTENDS Main ABSTRACT"))
          .execute();
      databaseDocumentTx.command(new OCommandSQL("CREATE CLASS Derived EXTENDS Main")).execute();
      databaseDocumentTx
          .command(new OCommandSQL("INSERT INTO Derived SET uuid='abcdef'"))
          .execute();
      databaseDocumentTx
          .command(new OCommandSQL("ALTER CLASS Derived SUPERCLASSES Base"))
          .execute();

      List<ODocument> results =
          databaseDocumentTx
              .command(new OCommandSQL("SELECT * FROM Derived WHERE uuid='abcdef'"))
              .execute();
      assertEquals(results.size(), 1);

    } finally {
      databaseDocumentTx.drop();
    }
  }

  @Test
  public void testListContainsField() {
    ODatabaseDocumentTx databaseDocumentTx =
        new ODatabaseDocumentTx("memory:OCommandExecutorSQLSelectTestIndex_testListContainsField");
    databaseDocumentTx.create();
    try {
      databaseDocumentTx.command(new OCommandSQL("CREATE CLASS Foo")).execute();
      databaseDocumentTx.command(new OCommandSQL("CREATE PROPERTY Foo.name String")).execute();
      databaseDocumentTx.command(new OCommandSQL("INSERT INTO Foo SET name = 'foo'")).execute();

      List<?> result =
          databaseDocumentTx.query(
              new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name"));
      assertEquals(result.size(), 1);

      result =
          databaseDocumentTx.query(
              new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE name IN ['foo', 'bar']"));
      assertEquals(result.size(), 1);

      databaseDocumentTx
          .command(new OCommandSQL("CREATE INDEX Foo.name UNIQUE_HASH_INDEX"))
          .execute();

      result =
          databaseDocumentTx.query(
              new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE ['foo', 'bar'] CONTAINS name"));
      assertEquals(result.size(), 1);

      result =
          databaseDocumentTx.query(
              new OSQLSynchQuery<Object>("SELECT * FROM Foo WHERE name IN ['foo', 'bar']"));
      assertEquals(result.size(), 1);

    } finally {
      databaseDocumentTx.drop();
    }
  }
}
