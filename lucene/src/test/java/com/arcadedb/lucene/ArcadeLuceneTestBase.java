package com.arcadedb.lucene;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import java.io.File;

public abstract class ArcadeLuceneTestBase {

    protected Database database;
    protected DatabaseFactory databaseFactory;
    protected static final String DB_PATH_PREFIX = "target/databases/testLucene_";

    @BeforeEach
    public void setUp() {
        GlobalConfiguration.TX_WAL.setValue(false);
        GlobalConfiguration.WAL_FLUSH.setValue(WALFile.FLUSH_METHOD.NONE);
        String currentDbPath = DB_PATH_PREFIX + System.nanoTime();
        databaseFactory = new DatabaseFactory(currentDbPath);
        database = databaseFactory.create();
    }

    @AfterEach
    public void tearDown() {
        if (database != null && database.isOpen()) {
            database.close();
        }
        if (databaseFactory != null) {
            databaseFactory.drop();
        }
    }
}
