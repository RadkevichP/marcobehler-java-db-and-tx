package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeadlockInsert2Exercise {

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deadlock_insert_exercise_part1() throws SQLException {
        System.out.println("Do we reach the end of the test without a deadlock?....");
        try (Connection connectionFromPavel = getConnection()) {
            connectionFromPavel.setAutoCommit(false);
            connectionFromPavel.createStatement().execute(
                    "insert into items (id, name) values (1, 'CTU Filed Agent Report')"
            );
            try (Connection connectionFromVova = getConnection()) {
                connectionFromVova.setAutoCommit(false);
                connectionFromVova.createStatement().execute(
                        "insert into items (id, name) values (2, 'CTU Filed Agent Report2')"
                );
                try (Connection connectionFromMax = getConnection()) {
                    connectionFromMax.setAutoCommit(false);
                    connectionFromMax.createStatement().execute(
                            "insert into items (id, name) values (3, 'CTU Filed Agent Report2')"
                    );
                }
            }
        }
        System.out.println("Yes!");
        try (Connection checkingConnection = getConnection()) {
            System.out.println(getItemsCount(checkingConnection) + " items found!");
        }

    }


    private int getItemsCount(Connection connection) throws SQLException {
        ResultSet resultSet = connection.createStatement()
                .executeQuery("select count(*) as count from items");
        resultSet.next();
        int count = resultSet.getInt("count");
        System.out.println("Items in the items table: " + count);
        resultSet.close();
        return count;
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
                    "(id identity, name VARCHAR unique)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}