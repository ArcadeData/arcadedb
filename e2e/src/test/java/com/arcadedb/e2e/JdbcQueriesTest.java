package com.arcadedb.e2e;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcQueriesTest extends ArcadeContainerTemplate {

    private Connection conn;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    @BeforeEach
    void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "playwithdata");
        props.setProperty("ssl", "false");

        conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + pgsqlPort + "/beer", props);
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    void simpleSQLQuery() throws Exception {

        try (Statement st = conn.createStatement()) {

            try (java.sql.ResultSet rs = st.executeQuery("SELECT * FROM Beer limit 1")) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getString("name")).isNotBlank();

                assertThat(rs.next()).isFalse();
            }
        }
    }
}
