package part_01_jdbc.code;

import org.h2.jdbc.JdbcSQLException;
import org.h2.jdbc.JdbcSQLTimeoutException;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class PessimisticLockingExercise {

    @Before
    public void setup() {
        try (Connection connection = getConnection()) {
            createTables(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = JdbcSQLTimeoutException.class)
    public void pessimistic_locking_exercise() throws SQLException {
        // first, jack bauer inserts a field agent report
        int resultKey;
        try (Connection connectionFromJackBauer = getConnection()) {
            connectionFromJackBauer.setAutoCommit(false);
            Statement statement = connectionFromJackBauer.createStatement();
            String query = "insert into items (name, release_date) values ('CTU Field Agent Report', current_date() - 100)";
            statement.execute(query, Statement.RETURN_GENERATED_KEYS);
            connectionFromJackBauer.commit();
            ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            resultKey = rs.getInt(1);
            System.out.println(resultKey);
        }

        try (Connection connectionFromJackBauer = getConnection()) {
            connectionFromJackBauer.setAutoCommit(false);
            // later on jack wants to update the field report and make
            // sure noone else can access the rows at the same time!
            connectionFromJackBauer.createStatement()
                    .execute("select * from items where " +
                            "id = " + resultKey + " for update");
            // TODO update the row, etc.
            System.out.println("Jack Bauer locked the row for any " +
                    "other update");
            // then habib shows up and tries to update the row, but
            // cannot. An Exception is being thrown
            try (Connection connectionFromHabibMarwan = getConnection()) {
                connectionFromHabibMarwan.setAutoCommit(false);
                connectionFromHabibMarwan.createStatement()
                        .executeUpdate(
                                "update items set " +
                                        "release_date = current_date() + 10" +
                                        " where name = " +
                                        "'CTU Field Agent Report'");
                fail("We should never be able to get to this line, " +
                        "because an exception is thrown");
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:exercise_bd;DB_CLOSE_DELAY=-1");
    }

    private void createTables(Connection conn) {
        try {
            conn.createStatement().execute("create table bids (id " +
                    "identity, user VARCHAR, time TIMESTAMP ," +
                    " amount NUMBER, currency VARCHAR) ");
            conn.createStatement().execute("create table items (id " +
                    "identity, release_date date, name VARCHAR)");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
