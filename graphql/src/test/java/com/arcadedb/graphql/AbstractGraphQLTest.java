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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

public class AbstractGraphQLTest {
    protected static final String DB_PATH = "./target/testgraphql";

    @BeforeEach
    @AfterEach
    public void clean() {
        FileUtils.deleteRecursively(new File(DB_PATH));
    }

    protected void executeTest(final Callable<Void, Database> callback) {
        try (DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
            if (factory.exists())
                factory.open().drop();

            final Database database = factory.create();
            try {
                database.transaction(() -> {
                    Schema schema = database.getSchema();
                    schema.getOrCreateVertexType("Book");
                    schema.getOrCreateVertexType("Author");
                    schema.getOrCreateEdgeType("IS_AUTHOR_OF");

                    MutableVertex author1 = database.newVertex("Author");
                    author1.set("id", "author-1");
                    author1.set("firstName", "Joanne");
                    author1.set("lastName", "Rowling");
                    author1.save();

                    MutableVertex book1 = database.newVertex("Book");
                    book1.set("id", "book-1");
                    book1.set("name", "Harry Potter and the Philosopher's Stone");
                    book1.set("pageCount", 223);
                    book1.save();

                    MutableVertex book2 = database.newVertex("Book");
                    book2.set("id", "book-2");
                    book2.set("name", "Mr. brain");
                    book2.set("pageCount", 422);
                    book2.save();

                    author1.newEdge("IS_AUTHOR_OF", book1, true);
                    author1.newEdge("IS_AUTHOR_OF", book2, true);
                });

                database.transaction(() -> callback.call(database));
            } finally {
                if (database.isTransactionActive())
                    database.rollback();
                database.drop();
            }
        }
    }

    protected int countIterable(Iterable<?> iter) {
        int count = 0;
        for (Object o : iter)
            ++count;

        return count;
    }

    protected void defineTypes(final Database database) {
        final String types = "type Query {\n" +//
                "  bookById(id: String): Book\n" +//
                "  bookByName(name: String): Book\n" +//
                "}\n\n" +//
                "type Book {\n" +//
                "  id: String\n" +//
                "  name: String\n" +//
                "  pageCount: Int\n" +//
                "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
                "}\n\n" +//
                "type Author {\n" +//
                "  id: String\n" +//
                "  firstName: String\n" +//
                "  lastName: String\n" +//
                "  wrote: [Book] @relationship(type: \"IS_AUTHOR_OF\", direction: OUT)\n" +//
                "}";
        database.command("graphql", types);
    }
}
