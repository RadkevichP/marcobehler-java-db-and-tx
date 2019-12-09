package part_01_jdbc.code;

import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DeadlockUpdateExercise {

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
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
                    "(id identity, name VARCHAR)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
