package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OptimisticLockingExercise {
    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = OptimisticLockingException.class)
    public void optimistic_locking_exercise() throws SQLException {
        try(Connection connectionPR = getConnection()) {
            connectionPR.setAutoCommit(false);
            connectionPR.createStatement().execute("insert into items " +
                    "(name, release_date, version) values " +
                    "('CTU Field Agent " +
                    "Report', current_date() - 100, 0)");
            int updatedRows = connectionPR.createStatement()
                    .executeUpdate("update items set release_date = current_date(), " +
                            " version = version + 1 " +
                            "where name = 'CTU Field Agent Report'" +
                            " and version = 0");

        }
    }


    public static class OptimisticLockingException extends RuntimeException {}

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:exercise_bd;DB_CLOSE_DELAY=-1");
    }

    private void createTables(Connection conn) {
        try {
            conn.createStatement().execute("create table bids (id " +
                    "identity, user VARCHAR, time TIMESTAMP ," +
                    " amount NUMBER, currency VARCHAR) ");
            conn.createStatement().execute("create table items (id " +
                    "identity, name VARCHAR, release_date date," +
                    " version NUMBER default " +
                    "0)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
