package org.svgroz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Simon Grozovsky svgroz@outlook.com
 */
public class Application {
    static final String URL = "jdbc:postgresql://localhost:5432/user";
    static final String USER = "user";
    static final String PASSWORD = "password";

    public static void main(String[] args) throws Exception {
        prepareDb();
        var connection1 = DriverManager.getConnection(URL, USER, PASSWORD);
        var connection2 = DriverManager.getConnection(URL, USER, PASSWORD);
        connection1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        connection1.setAutoCommit(false);

        {
            var statement = connection1.prepareStatement("SELECT * FROM test_table where name = ?");
            statement.setString(1, "foo");
            var rs = statement.executeQuery();
            while (rs.next()) {
                System.out.println(rsTo(rs));
            }
        }

        {
            connection2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection2.setAutoCommit(false);
            var  statement = connection2.prepareStatement("update test_table set account = ? where name = ?");
            statement.setLong(1, ThreadLocalRandom.current().nextInt());
            statement.setString(2, "bar");
            statement.executeUpdate();
            connection2.commit();
        }

        {
            var statement = connection1.prepareStatement("SELECT * FROM test_table where name = ?");
            statement.setString(1, "bar");
            var rs = statement.executeQuery();
            while (rs.next()) {
                System.out.println(rsTo(rs));
            }
        }
    }

    static String rsTo(ResultSet rs) throws Exception {
        var id = rs.getLong(1);
        var name = rs.getString(2);
        var acc = rs.getLong(3);
        return "id: " + id + " name: " + name + " acc: " + acc;
    }

    static void prepareDb() throws Exception {
        var connection = DriverManager.getConnection(URL, USER, PASSWORD);
        var statement = connection.prepareStatement(
                "DROP TABLE if EXISTS test_table;\n" +
                        "\n" +
                        "CREATE TABLE test_table (\n" +
                        "    id bigserial PRIMARY KEY,\n" +
                        "    name VARCHAR(100) not null,\n" +
                        "    account INTEGER NOT NULL\n" +
                        ");\n" +
                        "\n" +
                        "INSERT INTO test_table (name, account) values ('foo', 100);\n" +
                        "INSERT INTO test_table (name, account) values ('bar', 200);"
        );
        statement.executeUpdate();
    }
}
