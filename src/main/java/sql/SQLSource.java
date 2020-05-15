package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLSource {

    private static SQLSource instance;
    private String ip;
    private String url;
    private String user;
    private String password;

    private SQLSource() {
        //URL needs to be changed accordingly when the IP for the ec2 changes
        ip = "18.221.35.79";
        url = System.getProperty("database.url", "jdbc:postgresql://"+ip+":5432/pokemondb");
        user = System.getProperty("database.username", "pokemondb");
        password = System.getProperty("database.password", "pokemondb");
    }

    public static SQLSource getInstance() {
        if (instance == null) {
            instance = new SQLSource();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
    
}