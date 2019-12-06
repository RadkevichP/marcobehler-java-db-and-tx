package part_01_jdbc.code;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Exercise1Java7 {

    private static final Integer NO_TIMEOUT = 0;

    @Test
    public void open_jdbc_connection_java_7() {

        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:exercise_bd;DB_CLOSE_DELAY=-1"
        )) {
            System.out.println("Are we connected to the database& : "
                    + conn.isValid(NO_TIMEOUT));

            conn.createStatement().execute("create table bids " +
                    "(id identity, user VARCHAR, time TIMESTAMP, " +
                    "amount NUMBER, currency VARCHAR)");

            conn.createStatement().execute("create table items " +
                    "(id identity, name VARCHAR )");

            System.out.println("Yay, tables created!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
