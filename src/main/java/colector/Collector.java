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
                logger.info("ѕуть не найден :" + path );
            }

        }

//connectionDb("jdbc:ucanaccess://c:base.mdb", "jdbc:ucanaccess://c:db2.mdb");


    }
    public static void connectionDb(String urlSource, String urlDestination){
       try{
           Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
           logger.info("ѕодключение к базе данных источника :" + urlSource );
           Connection dbConnOnSrcDB = DriverManager.getConnection(urlSource);
           logger.info("ѕодключение к базе данных " + urlSource+ " прошло успешно");
           logger.info("ѕодключение к базе данных назначени€ :" + urlDestination );
           Connection dbConnOnDestDB = DriverManager.getConnection(urlDestination);
           logger.info("ѕодключение к базе данных " + urlDestination + " прошло успешно");
           String sqlQuery = "SELECT * FROM Cable";
             copyTableData(dbConnOnSrcDB, dbConnOnDestDB, sqlQuery, "Cable_11");

              insertTable(dbConnOnDestDB);
             clearTable(dbConnOnDestDB);
           dbConnOnSrcDB.close();
           dbConnOnDestDB.close();
           
           logger.info("копирование завершено успешно");

       } catch (ClassNotFoundException | SQLException e) {
          logger.error(e.getMessage());
       }

    }

      //копирует данные из тблицы во временную таблицу
    public static void copyTableData(Connection dbConnOnSrcDB, Connection dbConnOnDestDB,
                              String sqlQueryOnSrcDB, String tableNameOnDestDB) {
        logger.info("копирование во временную таблицу: " + tableNameOnDestDB);
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
                logger.info("выполнено копирование во временную таблицу : " + tableNameOnDestDB);
            }catch (SQLException e){
                logger.error(e.toString());
            }
        } catch (SQLException e) {
           logger.error(e.toString());
        }
    }
    //вставка   таблицы(временной) из источника в таблицу назначени€ игнориру€ повторы по полю serial (уникальный заводской номер)
    public static void insertTable ( Connection dbConnOnDestDB)  {

        String sqlQuery = "INSERT INTO  Cable  select * from Cable_11 where cable_11.serial not in (select cable.serial from Cable)" ;
        logger.info("¬ставка из из временной таблицы начата..");
        try (PreparedStatement prepSqlStatmOnDestDB = dbConnOnDestDB.prepareStatement(sqlQuery )){
            prepSqlStatmOnDestDB.addBatch();
            prepSqlStatmOnDestDB.executeBatch();
            logger.info("¬ставка из из временной таблицы выполнена");
        } catch (SQLException e) {
            logger.error(e.toString());
        }

    }
// очищает временную табицу
    public static void clearTable (Connection dbConnOnDestDB){
        String sqlQuery = "Delete from Cable_11" ;
        logger.info("ќчистка временной таблицы");

        try (PreparedStatement prepSqlStatmOnDestDB = dbConnOnDestDB.prepareStatement(sqlQuery )){
            prepSqlStatmOnDestDB.addBatch();
            prepSqlStatmOnDestDB.executeBatch();
            logger.info("ќчистка временной таблицы выполнена");
        } catch (SQLException e) {
            logger.error(e.toString());
        }
    }

    public Collector() {

    }
}
