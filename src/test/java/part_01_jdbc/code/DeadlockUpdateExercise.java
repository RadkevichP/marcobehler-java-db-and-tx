package part_01_jdbc.code;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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

    @Test
    public void dedlock_update_exercise() throws SQLException {
        System.out.println("Do we reach the end of the test without a deadlock?....");
        try (Connection connectionPR = getConnection()) {
            connectionPR.setAutoCommit(false);
            connectionPR.createStatement().execute("insert into items " +
                    "(name) values ('CTU Field Report')");
            try (Connection connectionHM = getConnection()) {
                connectionHM.setAutoCommit(false);
                connectionHM.createStatement().execute("update items set name = 'destroyed'" +
                        " where name = 'CTU Field Report' ;");
               /* connectionHM.createStatement().execute("delete from items where name = 'CTU Field Report'");*/
                connectionHM.commit();
            }
            connectionPR.commit();
        }
        System.out.println("Yes!");

        try (Connection checkConnection = getConnection()){
            ResultSet resultSet = checkConnection.createStatement()
                    .executeQuery("select * from items");
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
                    "(id identity, name VARCHAR unique )");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
