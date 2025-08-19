package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class FastTextVectorImportTest
{
    @Test
    public void vectorNeighborsQuery() {
        final String databasePath = "target/databases/test-fasttextsmall";

        FileUtils.deleteRecursively(new File(databasePath));

        final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
        if (databaseFactory.exists())
            databaseFactory.open().drop();

        final Database db = databaseFactory.create();
        try {
            db.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                    + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
                    + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name" //
            );
            assertThat(db.countType("Word", true)).isEqualTo(1000);

            final ResultSet rs = db.command("SQL",
                    "select expand(vectorNeighbors('Word[name,vector]','with',10))");

            final AtomicInteger total = new AtomicInteger();
            while (rs.hasNext()) {
                final Result record = rs.next();
                assertThat(record).isNotNull();
                Vertex vertex = (Vertex) record.getElementProperty("vertex");
                Float distance = record.getProperty("distance");
                total.incrementAndGet();
            }

            assertThat(total.get()).isEqualTo(10);
        } finally {
            db.drop();
            TestHelper.checkActiveDatabases();
            FileUtils.deleteRecursively(new File(databasePath));
        }
    }

    @Test
    public void parsingLimitEntries() {
        final String databasePath = "target/databases/test-fasttextsmall";

        FileUtils.deleteRecursively(new File(databasePath));

        final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
        if (databaseFactory.exists())
            databaseFactory.open().drop();

        final Database db = databaseFactory.create();
        try {
            db.command("sql", "import database file://src/test/resources/cc.en.300.small.vec.gz "  //
                    + "with distanceFunction = cosine, m = 16, ef = 128, efConstruction = 128, " //
                    + "vertexType = Word, edgeType = Proximity, vectorProperty = vector, idProperty = name, "
                    + "parsingLimitEntries = 101"
            );

            // The header is skipped, so we expect 100 entries
            assertThat(db.countType("Word", true)).isEqualTo(100);
        } finally {
            db.drop();
            TestHelper.checkActiveDatabases();
            FileUtils.deleteRecursively(new File(databasePath));
        }
    }
}
