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
import org.assertj.core.api.Assertions;


import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class BaseGraphTest extends TestHelper {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  protected static final String EDGE3_TYPE_NAME   = "E3";
  protected static final String DB_PATH           = "target/databases/graph";

  protected static RID root;

  @Override
  public void beginTest() {
    GlobalConfiguration.resetAll();
    FileUtils.deleteRecursively(new File(DB_PATH));

    database.transaction(() -> {
      assertThat(database.getSchema().existsType(VERTEX1_TYPE_NAME)).isFalse();
      database.getSchema().buildVertexType().withName(VERTEX1_TYPE_NAME).withTotalBuckets(3).create();

      assertThat(database.getSchema().existsType(VERTEX2_TYPE_NAME)).isFalse();
      database.getSchema().buildVertexType().withName(VERTEX2_TYPE_NAME).withTotalBuckets(3).create();

      database.getSchema().buildEdgeType().withName(EDGE1_TYPE_NAME).create();
      database.getSchema().buildEdgeType().withName(EDGE2_TYPE_NAME).create();
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
    final MutableEdge e1 = v1.newEdge(EDGE1_TYPE_NAME, v2, "name", "E1");
    assertThat(v1).isEqualTo(e1.getOut());
    assertThat(v2).isEqualTo(e1.getIn());

    final MutableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
    v3.set("name", "V3");
    v3.save();

    assertThat(v3.asVertex()).isEqualTo(v3);
    assertThat(v3.asVertex(true)).isEqualTo(v3);
    try {
      assertThat(v3.asEdge()).isNotNull();
      fail("");
    } catch (final ClassCastException e) {
      // EXPECTED
    }
    try {
      assertThat(v3.asEdge(true)).isNotNull();
      fail("");
    } catch (final ClassCastException e) {
      // EXPECTED
    }

    final Map<String, Object> params = new HashMap<>();
    params.put("name", "E2");

    // CREATION OF EDGE PASSING PARAMS AS MAP
    final MutableEdge e2 = v2.newEdge(EDGE2_TYPE_NAME, v3, params);
    assertThat(v2).isEqualTo(e2.getOut());
    assertThat(v3).isEqualTo(e2.getIn());

    assertThat(e2.asEdge()).isEqualTo(e2);
    assertThat(e2.asEdge(true)).isEqualTo(e2);

    try {
      assertThat(e2.asVertex()).isNotNull();
      fail("");
    } catch (final ClassCastException e) {
      // EXPECTED
    }
    try {
      assertThat(e2.asVertex(true)).isNotNull();
      fail("");
    } catch (final ClassCastException e) {
      // EXPECTED
    }

    final ImmutableLightEdge e3 = v1.newLightEdge(EDGE2_TYPE_NAME, v3);
    assertThat(v1).isEqualTo(e3.getOut());
    assertThat(v3).isEqualTo(e3.getIn());

    db.commit();

    root = v1.getIdentity();
  }
}
