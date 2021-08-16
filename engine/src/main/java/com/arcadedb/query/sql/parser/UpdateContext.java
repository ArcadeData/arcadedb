/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.parser;

/**
 * This class is used by modifiers to return the documents that have to be updated AND the field that has to be updated.
 * Further operatios are then performed by the top level UpdateItem
 * <p>
 * Eg.
 * <p>
 * UPDATE Foo SET foo.bar.baz.name = 'xx'
 * <p>
 * the chain is following:
 * <p>
 * (identifier: foo) -> (modifier: bar) -> (modifier: baz) -> (modifier: name)
 * <p>
 * The top level UpdateItem calculates the value foo and will pass it to the modifier.
 * The modifier calculats the value of &ltdocsToUpdate&gt; = foo.bar.baz (that is a collection) an returns
 * to the top level UpdateItem an UpdateContext containig { docsToUpdate = &ltdocsToUpdate&gt;, fieldToSet = 'name'}
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpdateContext {
  public Iterable   docsToUpdate;
  public Identifier fieldToSet;
}
