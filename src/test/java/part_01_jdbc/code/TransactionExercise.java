package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionExercise {

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void transaction_exercise() {
        try (Connection connection = getConnection()) {
            System.out.println("Opening a JDBC transaction ....");

            connection.setAutoCommit(false);

            connection.createStatement().execute("insert into items (name)"
                    + " values ('Windows 10 Premium Edition')");
            connection.createStatement().execute("insert into bids (user,"
                    + " time, amount, currency) values ('Franz',now() ," +
                    " 2" + ", 'EUR')");
            connection.createStatement().execute("insert into bidz (user,"
                    + " time, amount, currency) values ('Peter',now() ," +
                    " 3" + ", 'USD')");
            connection.createStatement().execute("insert into bids (user,"
                    + " time, amount, currency) values ('Cristof',now() ," +
                    " 8" + ", 'EUR')");
            connection.commit();

            System.out.println("Commit is done! Now everything is in the DB");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection connection = getConnection()) {
            System.out.println("Reading from bids.....");
            ResultSet rs = connection.createStatement().executeQuery("select * from bids");
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
