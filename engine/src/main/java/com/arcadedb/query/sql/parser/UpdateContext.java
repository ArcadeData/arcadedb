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
 */
package com.arcadedb.query.sql.parser;

/**
 * This class is used by modifiers to return the documents that have to be updated AND the field that has to be updated.
 * Further operations are then performed by the top level UpdateItem
 * <p>
 * Eg.
 * <p>
 * UPDATE Foo SET foo.bar.baz.name = 'xx'
 * <p>
 * the chain is following:
 * <p>
 * (identifier: foo) -&gt; (modifier: bar) -&gt; (modifier: baz) -&gt; (modifier: name)
 * <p>
 * The top level UpdateItem calculates the value foo and will pass it to the modifier.
 * The modifier calculates the value of &lt;docsToUpdate&gt; = foo.bar.baz (that is a collection) and returns
 * to the top level UpdateItem an UpdateContext containing { docsToUpdate = &lt;docsToUpdate&gt;, fieldToSet = 'name'}
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UpdateContext {
//  public Iterable   docsToUpdate;
//  public Identifier fieldToSet;
}
