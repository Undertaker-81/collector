package colector;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Collector {
    private static final Logger logger = LoggerFactory.getLogger(Collector.class);

    public static void main(String[] args)  {
        for (String path : args){
            try {
                connectionDb("jdbc:ucanaccess://" + path, "jdbc:ucanaccess://c:\\db2.mdb");
            }catch (Exception e){
                logger.info("���� �� ������ :" + path );
            }

        }

//connectionDb("jdbc:ucanaccess://c:base.mdb", "jdbc:ucanaccess://c:db2.mdb");


    }
    public static void connectionDb(String urlSource, String urlDestination){
       try{
           Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
           logger.info("����������� � ���� ������ ��������� :" + urlSource );
           Connection dbConnOnSrcDB = DriverManager.getConnection(urlSource);
           logger.info("����������� � ���� ������ " + urlSource+ " ������ �������");
           logger.info("����������� � ���� ������ ���������� :" + urlDestination );
           Connection dbConnOnDestDB = DriverManager.getConnection(urlDestination);
           logger.info("����������� � ���� ������ " + urlDestination + " ������ �������");
           String sqlQuery = "SELECT * FROM Cable";
             copyTableData(dbConnOnSrcDB, dbConnOnDestDB, sqlQuery, "Cable_11");

              insertTable(dbConnOnDestDB);
             clearTable(dbConnOnDestDB);
           dbConnOnSrcDB.close();
           dbConnOnDestDB.close();
           
           logger.info("����������� ��������� �������");

       } catch (ClassNotFoundException | SQLException e) {
          logger.error(e.getMessage());
       }

    }

      //�������� ������ �� ������ �� ��������� �������
    public static void copyTableData(Connection dbConnOnSrcDB, Connection dbConnOnDestDB,
                              String sqlQueryOnSrcDB, String tableNameOnDestDB) {
        logger.info("����������� �� ��������� �������: " + tableNameOnDestDB);
        try (PreparedStatement prepSqlStatmOnSrcDB = dbConnOnSrcDB.prepareStatement(sqlQueryOnSrcDB);

                ResultSet sqlResultsFromSrcDB = prepSqlStatmOnSrcDB.executeQuery()) {
            ResultSetMetaData sqlMetaResults = sqlResultsFromSrcDB.getMetaData();

            // Stores the query results
            List<String> columnsOfQuery = new ArrayList<>();

            // Store query results
            for (int i = 1; i <= sqlMetaResults.getColumnCount(); i++)
                columnsOfQuery.add(sqlMetaResults.getColumnName(i));

            try (PreparedStatement prepSqlStatmOnDestDB = dbConnOnDestDB.prepareStatement(
                            "INSERT INTO " + tableNameOnDestDB +
                                    " (" + columnsOfQuery.stream().collect(Collectors.joining(", ")) + ") " +
                                    "VALUES (" + columnsOfQuery.stream().map(c -> "?").collect(Collectors.joining(", ")) + ") ")) {

                while (sqlResultsFromSrcDB.next()) {
                    for (int i = 1; i <= sqlMetaResults.getColumnCount(); i++)
                        prepSqlStatmOnDestDB.setObject(i, sqlResultsFromSrcDB.getObject(i));

                    prepSqlStatmOnDestDB.addBatch();
                }
                prepSqlStatmOnDestDB.executeBatch();
                logger.info("��������� ����������� �� ��������� ������� : " + tableNameOnDestDB);
            }catch (SQLException e){
                logger.error(e.toString());
            }
        } catch (SQLException e) {
           logger.error(e.toString());
        }
    }
    //�������   �������(���������) �� ��������� � ������� ���������� ��������� ������� �� ���� serial (���������� ��������� �����)
    public static void insertTable ( Connection dbConnOnDestDB)  {

        String sqlQuery = "INSERT INTO  Cable  select * from Cable_11 where cable_11.serial not in (select cable.serial from Cable)" ;
        logger.info("������� �� �� ��������� ������� ������..");
        try (PreparedStatement prepSqlStatmOnDestDB = dbConnOnDestDB.prepareStatement(sqlQuery )){
            prepSqlStatmOnDestDB.addBatch();
            prepSqlStatmOnDestDB.executeBatch();
            logger.info("������� �� �� ��������� ������� ���������");
        } catch (SQLException e) {
            logger.error(e.toString());
        }

    }
// ������� ��������� ������
    public static void clearTable (Connection dbConnOnDestDB){
        String sqlQuery = "Delete from Cable_11" ;
        logger.info("������� ��������� �������");

        try (PreparedStatement prepSqlStatmOnDestDB = dbConnOnDestDB.prepareStatement(sqlQuery )){
            prepSqlStatmOnDestDB.addBatch();
            prepSqlStatmOnDestDB.executeBatch();
            logger.info("������� ��������� ������� ���������");
        } catch (SQLException e) {
            logger.error(e.toString());
        }
    }

    public Collector() {

    }
}
