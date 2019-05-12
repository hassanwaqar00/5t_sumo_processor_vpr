package it.polito.5t.sumo.processor.vpr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBHelper {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://DB_IP_HERE:DB_PORT_HERE?useSSL=false&serverTimezone=UTC";
    static final String DB_USERNAME = "DB_USER_NAME_HERE";
    static final String DB_PASSWORD = "DB_PASSWORD_HERE";
    private Connection connection = null;

    public DBHelper() {
        jdbcConnect();
    }

    public Connection jdbcConnect() {
        try {
            Class.forName(JDBC_DRIVER).newInstance();
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(DBHelper.class.getName() + ".jdbcConnect").log(Level.WARNING, ex.toString());
            connection = null;
        }
        return connection;
    }

    public void jdbcClose() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {
                Logger.getLogger(DBHelper.class.getName() + ".jdbcClose").log(Level.WARNING, ex.toString());
            }
            connection = null;
        }
    }

    public List<Float> queryHourlyAvgFlow(String date, String pr, String step) throws SQLException {
        String query = "SELECT FLOOR(stepFrom/" + Main.STEP_SIZE + ") AS HOUR, AVG(flowNorth) "
                + "FROM sumo_processed_averagespeed.`" + date + "_" + pr + "pr_" + step + "s` "
                + "WHERE flowNorth IS NOT NULL "
                + "GROUP BY HOUR "
                + "ORDER BY stepFrom";
        connection.setCatalog("sumo_processed_averagespeed");
        Statement statement;
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        List<Float> flow = new ArrayList<>();
        while (rs.next()) {
            flow.add(rs.getFloat("AVG(flowNorth)"));
        }
        statement.close();
        statement = null;
        return flow;
    }

    public void selectAndInsertFromProcessedAveragespeed(String date, String pr, String step, int stepFrom, int stepTo, String newPr) throws SQLException {
        String query = "REPLACE INTO sumo_processed_averagespeed.`" + date + "_" + newPr + "prd_" + step + "s` (`stepFrom`, `stepTo`, `flowNorth`, `flowSouth`, `VIL-A`, `VIL-B`, `VIL-C`, `VIL-D`, `VIL-E`, `VIL-F`, `VIL-INT`, `VIL-INT_A2C`, `VIL-INT_B2C`, `VIL-INT_D2C`, `VIL-INT_C2A`, `VIL-INT_B2A`, `VIL-INT_D2A`, `VIL-INT_B2D`, `VIL-INT_D2B`, `PR`)"
                + "SELECT `stepFrom`, `stepTo`, `flowNorth`, `flowSouth`, `VIL-A`, `VIL-B`, `VIL-C`, `VIL-D`, `VIL-E`, `VIL-F`, `VIL-INT`, `VIL-INT_A2C`, `VIL-INT_B2C`, `VIL-INT_D2C`, `VIL-INT_C2A`, `VIL-INT_B2A`, `VIL-INT_D2A`, `VIL-INT_B2D`, `VIL-INT_D2B`, \"" + pr + "\" AS `PR` "
                + "FROM sumo_processed_averagespeed.`" + date + "_" + pr + "prd_" + step + "s` "
                + "WHERE stepFrom BETWEEN " + stepFrom + " AND " + stepTo + " "
                + "ORDER BY stepFrom";
        connection.setCatalog("sumo_processed_averagespeed");
        Statement statement;
        statement = connection.createStatement();
        statement.execute(query);
        statement.close();
        statement = null;
    }

    public void selectAndInsertFromProcessedDeltaTime(String date, String pr, String step, int stepFrom, int stepTo, String newPr) throws SQLException {
        String query = "REPLACE INTO sumo_processed_deltatime.`" + date + "_" + newPr + "prd_" + step + "s`(`stepFrom`, `stepTo`, `flowNorth`, `flowSouth`, `deltaTimeA2C`, `deltaTimeB2C`, `deltaTimeD2C`, `deltaTimeC2A`, `deltaTimeD2A`, `deltaTimeB2A`, `deltaTimeB2D`, `deltaTimeD2B`, `PR`) "
                + "SELECT `stepFrom`, `stepTo`, `flowNorth`, `flowSouth`, `deltaTimeA2C`, `deltaTimeB2C`, `deltaTimeD2C`, `deltaTimeC2A`, `deltaTimeD2A`, `deltaTimeB2A`, `deltaTimeB2D`, `deltaTimeD2B`, \"" + pr + "\" AS `PR` "
                + "FROM sumo_processed_deltatime.`" + date + "_" + pr + "prd_" + step + "s` "
                + "WHERE stepFrom BETWEEN " + stepFrom + " AND " + stepTo + " "
                + "ORDER BY stepFrom";
        connection.setCatalog("sumo_processed_deltatime");
        Statement statement;
        statement = connection.createStatement();
        statement.execute(query);
        statement.close();
        statement = null;

    }

    public int queryMaxStep(String date, String pr, String step) throws SQLException {
        String query = "SELECT max(stepTo) FROM sumo_processed_averagespeed.`" + date + "_" + pr + "prd_" + step + "s`";
        connection.setCatalog("sumo");
        Statement statement;
        statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        int maxStep = -1;
        while (rs.next()) {
            maxStep = rs.getInt("max(stepTo)");
        }
        statement.close();
        statement = null;
        return maxStep;
    }
    