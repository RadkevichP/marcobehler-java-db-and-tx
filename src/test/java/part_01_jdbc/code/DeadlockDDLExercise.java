package part_01_jdbc.code;

import org.h2.jdbc.JdbcSQLTimeoutException;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeadlockDDLExercise {

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = JdbcSQLTimeoutException.class)
    public void deadlockDDL_exercise() throws SQLException {
        System.out.println("Do we reach the end of the test?...(No, if deadlock occurs)");
        try (Connection connectionPR = getConnection()) {
            connectionPR.setAutoCommit(false);
            connectionPR.createStatement().execute("insert into items" +
                    "(name) values ('CTU Field Report')");
            try (Connection connectionKV = getConnection()) {
                connectionKV.setAutoCommit(false);
                //connectionKV.createStatement().execute("alter table items add column (release_date date null)");
                //connectionKV.createStatement().execute("drop table items");
                //connectionKV.createStatement().execute("alter table items rename to test_items");
                connectionKV.createStatement().execute("truncate table items");
                connectionKV.commit();
            }
            connectionPR.commit();
        }
        System.out.println("Yes!");

        try (Connection checkConnection = getConnection()){
            ResultSet resultSet = checkConnection.createStatement()
                    .executeQuery("select * from test_items");
            resultSet.next();
            System.out.println("Name from DB: " + resultSet.getString("name"));
        }

    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:exercise_bd;DB_CLOSE_DELAY=-1");
    }

    private void createTables(Connection conn) {
        try {
            conn.createStatement().execute("create table bids " +
                    "(id identity, user VARCHAR, time TIMESTAMP, " +
                    "amount NUMBER, currency VARCHAR)");

            conn.createStatement().execute("create table items " +
                    "(id identity, name VARCHAR )");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
