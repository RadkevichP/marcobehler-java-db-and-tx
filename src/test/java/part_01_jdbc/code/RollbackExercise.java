package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RollbackExercise {

    private static final Integer NO_TIMEOUT = 0;

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void roolback_exercise() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(false);

            connection.createStatement().execute("insert into items " +
                    "(name) values ('Windows 10 Premium Edition')");

            connection.createStatement().execute("insert into bids" +
                    " (user, time, amount, currency) values ('Hans', " +
                    "now(), 1, 'EUR')");
            connection.createStatement().execute("insert into bids " +
                    "(user, time, amount, currency) values ('Franz'," +
                    "now() , 2, 'EUR')");
            //connection.rollback();
            throw new SQLException("test");
            //System.out.println("We rolled back our transaction!");
            //assertThat(getItemsCount(connection), equalTo(0));
        } catch (SQLException e) {
            Connection connection = getConnection();
            System.out.println("Are we connected to the database : "
                    + connection.isValid(NO_TIMEOUT));
            assertThat(getItemsCount(connection), equalTo(0));
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
                    "(id identity, name VARCHAR )");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
