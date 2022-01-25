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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseGraphTest extends TestHelper {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  protected static final String DB_PATH           = "target/databases/graph";

  protected static RID root;

  @Override
  public void beginTest() {
    GlobalConfiguration.resetAll();
    FileUtils.deleteRecursively(new File(DB_PATH));

    database.transaction(() -> {
      Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));
      database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);

      Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
      database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

      database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
      database.getSchema().createEdgeType(EDGE2_TYPE_NAME);
    });

    final Database db = database;
    db.begin();

    final MutableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
    v1.set("name", VERTEX1_TYPE_NAME);
    v1.save();

    final MutableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
    v2.set("name", VERTEX2_TYPE_NAME);
    v2.save();

    // CREATION OF EDGE PASSING PARAMS AS VARARGS
    MutableEdge e1 = v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
    Assertions.assertEquals(e1.getOut(), v1);
    Assertions.assertEquals(e1.getIn(), v2);

    final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
    v3.set("name", "V3");
    v3.save();

    Assertions.assertEquals(v3, v3.asVertex());
    Assertions.assertEquals(v3, v3.asVertex(true));
    try {
      Assertions.assertNotNull(v3.asEdge());
      Assertions.fail();
    } catch (ClassCastException e) {
      // EXPECTED
    }
    try {
      Assertions.assertNotNull(v3.asEdge(true));
      Assertions.fail();
    } catch (ClassCastException e) {
      // EXPECTED
    }

    Map<String, Object> params = new HashMap<>();
    params.put("name", "E2");

    // CREATION OF EDGE PASSING PARAMS AS MAP
    MutableEdge e2 = v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
    Assertions.assertEquals(e2.getOut(), v2);
    Assertions.assertEquals(e2.getIn(), v3);

    Assertions.assertEquals(e2, e2.asEdge());
    Assertions.assertEquals(e2, e2.asEdge(true));

    try {
      Assertions.assertNotNull(e2.asVertex());
      Assertions.fail();
    } catch (ClassCastException e) {
      // EXPECTED
    }
    try {
      Assertions.assertNotNull(e2.asVertex(true));
      Assertions.fail();
    } catch (ClassCastException e) {
      // EXPECTED
    }

    ImmutableLightEdge e3 = v1.newLightEdge(EDGE2_TYPE_NAME, v3, true);
    Assertions.assertEquals(e3.getOut(), v1);
    Assertions.assertEquals(e3.getIn(), v3);

    db.commit();

    root = v1.getIdentity();
  }
}
