package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AutocommitExcercise {

    private static final Integer NO_TIMEOUT = 0;

    @Before
    public void setup() {
        try (Connection connection = getConnection();) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void autocommit_true() {
        try (Connection conn = getConnection()) {
            conn.createStatement().execute("insert into items (name)"
                    + " values ('Windows 10 Premium Edition')");
            conn.createStatement().execute("insert into bids (user,"
                    + " time, amount, currency) values ('Franz',now() ," +
                    " 2" + ", 'EUR')");

            try {
                conn.createStatement().execute("insert into bidz (user,"
                        + " time, amount, currency) values ('Peter',now() ," +
                        " 3" + ", 'USD')");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            conn.createStatement().execute("insert into bids (user,"
                    + " time, amount, currency) values ('Cristof',now() ," +
                    " 8" + ", 'EUR')");
            System.out.println("Are we connected to the database& : "
                    + conn.isValid(NO_TIMEOUT));

            ResultSet rs = conn.createStatement().executeQuery("select * from bids");
            while (rs.next()) {
                System.out.println(rs.getString("id") + " :" + rs.getString("user"));
            }


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
                    "(id identity, name VARCHAR )");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
