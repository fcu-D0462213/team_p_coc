import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

/*
  Download the Postgres JDBC driver jar from https://jdbc.postgresql.org.

  Then, compile and run this example like so:

  $ export CLASSPATH=.:/path/to/postgresql.jar
  $ javac BasicExample.java && java BasicExample

  To build the javadoc:

  $ javadoc -package -cp .:./path/to/postgresql.jar BasicExample.java

  At a high level, this code consists of two classes:

  1. BasicExample, which is where the application logic lives.

  2. BasicExampleDAO, which is used by the application to access the
     data store.

*/

public class BasicExample {


    public static void main(String[] args) {

        // Configure the database connection.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName("192.168.48.184");
        ds.setPortNumber(26257);
        ds.setDatabaseName("project");
        ds.setUser("test");
        ds.setPassword(null);
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("BasicExample");

        // Create DAO.
        BasicExampleDAO dao = new BasicExampleDAO(ds);

        // Test our retry handling logic if FORCE_RETRY is true.  This
        // method is only used to test the retry logic.  It is not
        // necessary in production code.
        dao.testRetryHandling();


        dao.dropSchema();
        // Set up the 'accounts' table.
        dao.createSchema();

        dao.createView();

        dao.loadData();
    }
}

/**
 * Data access object used by 'BasicExample'.  Abstraction over some
 * common CockroachDB operations, including:
 * <p>
 * - Auto-handling transaction retries in the 'runSQL' method
 * <p>
 * - Example of bulk inserts in the 'bulkInsertRandomAccountData'
 * method
 */

class BasicExampleDAO {

    private static final int MAX_RETRY_COUNT = 3;
    private static final String SAVEPOINT_NAME = "cockroach_restart";
    private static final String RETRY_SQL_STATE = "40001";
    private static final boolean FORCE_RETRY = false;

    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DataSource ds;

    BasicExampleDAO(DataSource ds) {
        this.ds = ds;
    }

    /**
     * Used to test the retry logic in 'runSQL'.  It is not necessary
     * in production code.
     */
    void testRetryHandling() {
        if (this.FORCE_RETRY) {
            runSQL("SELECT crdb_internal.force_retry('1s':::INTERVAL)");
        }
    }

    /**
     * Run SQL code in a way that automatically handles the
     * transaction retry logic so we don't have to duplicate it in
     * various places.
     *
     * @param sqlCode a String containing the SQL code you want to
     *                execute.  Can have placeholders, e.g., "INSERT INTO accounts
     *                (id, balance) VALUES (?, ?)".
     * @param args    String Varargs to fill in the SQL code's
     *                placeholders.
     * @return Integer Number of rows updated, or -1 if an error is thrown.
     */
    public Integer runSQL(String sqlCode, String... args) {

        // This block is only used to emit class and method names in
        // the program output.  It is not necessary in production
        // code.
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement elem = stacktrace[2];
        String callerClass = elem.getClassName();
        String callerMethod = elem.getMethodName();

        int rv = 0;

        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // automatically issue transaction retries.
            connection.setAutoCommit(false);

            int retryCount = 0;

            while (retryCount < MAX_RETRY_COUNT) {

                Savepoint sp = connection.setSavepoint(SAVEPOINT_NAME);

                // This block is only used to test the retry logic.
                // It is not necessary in production code.  See also
                // the method 'testRetryHandling()'.
                if (FORCE_RETRY) {
                    forceRetry(connection); // SELECT 1
                }

                try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

                    // Loop over the args and insert them into the
                    // prepared statement based on their types.  In
                    // this simple example we classify the argument
                    // types as "integers" and "everything else"
                    // (a.k.a. strings).
                    for (int i = 0; i < args.length; i++) {
                        int place = i + 1;
                        String arg = args[i];

                        try {
                            int val = Integer.parseInt(arg);
                            pstmt.setInt(place, val);
                        } catch (NumberFormatException e) {
                            pstmt.setString(place, arg);
                        }
                    }

                    if (pstmt.execute()) {
                        // We know that `pstmt.getResultSet()` will
                        // not return `null` if `pstmt.execute()` was
                        // true
                        ResultSet rs = pstmt.getResultSet();
                        ResultSetMetaData rsmeta = rs.getMetaData();
                        int colCount = rsmeta.getColumnCount();

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);

                        while (rs.next()) {
                            for (int i = 1; i <= colCount; i++) {
                                String name = rsmeta.getColumnName(i);
                                String type = rsmeta.getColumnTypeName(i);

                                // In this "bank account" example we know we are only handling
                                // integer values (technically 64-bit INT8s, the CockroachDB
                                // default).  This code could be made into a switch statement
                                // to handle the various SQL types needed by the application.
                                if (type == "int8") {
                                    int val = rs.getInt(name);

                                    // This printed output is for debugging and/or demonstration
                                    // purposes only.  It would not be necessary in production code.
                                    System.out.printf("    %-8s => %10s\n", name, val);
                                }
                            }
                        }
                    } else {
                        int updateCount = pstmt.getUpdateCount();
                        rv += updateCount;

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);
                    }

                    connection.releaseSavepoint(sp);
                    connection.commit();
                    break;

                } catch (SQLException e) {

                    if (RETRY_SQL_STATE.equals(e.getSQLState())) {
                        System.out.printf("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n",
                                e.getSQLState(), e.getMessage(), retryCount);
                        connection.rollback(sp);
                        retryCount++;
                        rv = -1;
                    } else {
                        rv = -1;
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.printf("BasicExampleDAO.runSQL ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
            rv = -1;
        }

        return rv;
    }

    /**
     * Helper method called by 'testRetryHandling'.  It simply issues
     * a "SELECT 1" inside the transaction to force a retry.  This is
     * necessary to take the connection's session out of the AutoRetry
     * state, since otherwise the other statements in the session will
     * be retried automatically, and the client (us) will not see a
     * retry error. Note that this information is taken from the
     * following test:
     * https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/logictest/testdata/logic_test/manual_retry
     *
     * @param connection Connection
     */
    private void forceRetry(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
            statement.executeQuery();
        }
    }

    /**
     * Creates a fresh, empty accounts table in the database.
     */
    public void createSchema() {
        //runSQL("CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT, CONSTRAINT balance_gt_0 CHECK (balance >= 0))");
        String createWarehousesCmd = "CREATE TABLE IF NOT EXISTS" + " warehouse ("
                + " W_ID int, "
                + " W_NAME String, "
                + " W_STREET_1 String, "
                + " W_STREET_2 String, "
                + " W_CITY String, "
                + " W_STATE String, "
                + " W_ZIP String, "
                + " W_TAX decimal, "
                + " W_YTD decimal, "
                + " PRIMARY KEY (W_ID) "
                + " );";
        String createDistrictsCmd = "CREATE TABLE IF NOT EXISTS" + " district ("
                + " D_W_ID int, "
                + " D_ID int, "
                + " D_NAME String, "
                + " D_STREET_1 String, "
                + " D_STREET_2 String, "
                + " D_CITY String, "
                + " D_STATE String, "
                + " D_ZIP String, "
                + " D_TAX decimal, "
                + " D_YTD decimal, "
                + " D_NEXT_O_ID int, "
                + " D_NEXT_DELIVER_O_ID int, "
                + " PRIMARY KEY (D_W_ID, D_ID) "
                + " );";
        String createCustomersCmd = "CREATE TABLE IF NOT EXISTS" + " customer ("
                + " C_W_ID int, "
                + " C_D_ID int, "
                + " C_ID int, "
                + " C_FIRST String, "
                + " C_MIDDLE String, "
                + " C_LAST String, "
                + " C_STREET_1 String, "
                + " C_STREET_2 String, "
                + " C_CITY String, "
                + " C_STATE String, "
                + " C_ZIP String, "
                + " C_PHONE String, "
                + " C_SINCE timestamp, "
                + " C_CREDIT String, "
                + " C_CREDIT_LIM decimal, "
                + " C_DISCOUNT decimal, "
                + " C_BALANCE decimal, "
                + " C_YTD_PAYMENT float, "
                + " C_PAYMENT_CNT int, "
                + " C_DELIVERY_CNT int, "
                + " C_DATA String, "
                + " C_W_NAME String, "
                + " C_D_NAME String, "
                + " PRIMARY KEY (C_W_ID, C_D_ID, C_ID) "
                + " );";


        String createOrdersCmd = "CREATE TABLE IF NOT EXISTS" + " orders ("
                + " O_W_ID int, "
                + " O_D_ID int, "
                + " O_ID int, "
                + " O_C_ID int, "
                + " O_CARRIER_ID int, "
                + " O_OL_CNT decimal, "
                + " O_ALL_LOCAL decimal, "
                + " O_ENTRY_D timestamp, "
                + " PRIMARY KEY (O_W_ID, O_D_ID, O_ID DESC) "
                + " );";


        String createItemsCmd = "CREATE TABLE IF NOT EXISTS" + " item ("
                + " I_ID int, "
                + " I_NAME String, "
                + " I_PRICE decimal, "
                + " I_IM_ID int, "
                + " I_DATA String, "
                + " PRIMARY KEY (I_ID) "
                + " );";

        String createOrderLinesCmd = "CREATE TABLE IF NOT EXISTS" + " order_line ("
                + " OL_W_ID int, "
                + " OL_D_ID int, "
                + " OL_O_ID int, "
                + " OL_NUMBER int, "
                + " OL_I_ID int, "
                + " OL_DELIVERY_D timestamp, "
                + " OL_AMOUNT decimal, "
                + " OL_SUPPLY_W_ID int, "
                + " OL_QUANTITY decimal, "
                + " OL_DIST_INFO String, "
                + " OL_I_NAME String, "
                + " PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID DESC, OL_QUANTITY DESC, OL_NUMBER, OL_I_ID)"
                + " );";

        String createStocksCmd = "CREATE TABLE IF NOT EXISTS" + " stock ("
                + " S_W_ID int, "
                + " S_I_ID int, "
                + " S_QUANTITY decimal, "
                + " S_YTD decimal, "
                + " S_ORDER_CNT int, "
                + " S_REMOTE_CNT int, "
                + " S_DIST_01 String, "
                + " S_DIST_02 String, "
                + " S_DIST_03 String, "
                + " S_DIST_04 String, "
                + " S_DIST_05 String, "
                + " S_DIST_06 String, "
                + " S_DIST_07 String, "
                + " S_DIST_08 String, "
                + " S_DIST_09 String, "
                + " S_DIST_10 String, "
                + " S_DATA String, "
                + " S_I_NAME String, "
                + " S_I_PRICE decimal, "
                + " PRIMARY KEY (S_W_ID, S_I_ID) "
                + " );";
        //runSQL("CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT, CONSTRAINT balance_gt_0 CHECK (balance >= 0))");
        runSQL(createWarehousesCmd);
        System.out.println("Successfully created table : warehouse");
        runSQL(createDistrictsCmd);
        System.out.println("Successfully created table : district");
        runSQL(createCustomersCmd);
        System.out.println("Successfully created table : customer");
        runSQL(createOrdersCmd);
        System.out.println("Successfully created table : orders");
        runSQL(createItemsCmd);
        System.out.println("Successfully created table : item");
        runSQL(createOrderLinesCmd);
        System.out.println("Successfully created table : order_line");
        runSQL(createStocksCmd);
        System.out.println("Successfully created table : stock");

        System.out.println("All tables are created successfully.");
    }


    public void createView() {
        String create_view_customer_top_balance = "CREATE VIEW" + " customer_sort_by_balance AS "
                + " SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_NAME, C_D_NAME FROM " + "customer "
                + " WHERE C_W_ID IS NOT NULL AND C_D_ID IS NOT NULL AND C_ID IS NOT NULL AND C_BALANCE IS NOT NULL "
                + " ORDER BY C_BALANCE DESC "
                + " ;";
        String create_view_order_by_customer_q = "CREATE VIEW" + " order_sort_by_customer AS "
                + " SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_ENTRY_D FROM " + "orders "
                + " WHERE O_W_ID IS NOT NULL AND O_D_ID IS NOT NULL AND O_ID IS NOT NULL AND O_C_ID IS NOT NULL "
                + " ORDER BY O_C_ID DESC, O_ID DESC "
                + " ;";
        String create_order_line_by_item_q = "CREATE VIEW" + " order_line_by_item AS "
                + " SELECT ol_w_id, ol_d_id, ol_o_id, ol_quantity, ol_number, ol_i_id FROM " + "order_line "
                + " WHERE ol_w_id IS NOT NULL AND ol_d_id IS NOT NULL AND ol_o_id IS NOT NULL AND ol_quantity IS NOT NULL AND ol_number IS NOT NULL AND ol_i_id IS NOT NULL "
                + " ;";
        runSQL(create_view_customer_top_balance);
        runSQL(create_view_order_by_customer_q);
        runSQL(create_order_line_by_item_q);
        System.out.println("Successfully created materialized view : customer_top_balance");
        System.out.println("Successfully created materialized view : create_view_order_by_customer_q");
        System.out.println("Successfully created materialized view : order_line_by_item");
    }

    public void loadData() {
        loadWarehouse();
        loadDistricts();
        loadCustomer2();
        loadOrder2();
        loadItem2();
        loadOrderLines2();
        loadStock2();
        //loadCustomer();
        //loadOrders();
        //loadItemsAndOrderLines();
        //loadStock();
        System.out.println("All data are loaded successfully.");
    }

    private void loadWarehouse() {
        String insertWarehousesCmd = "INSERT INTO " + " warehouse ("
                + " W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, "
                + " W_STATE, W_ZIP, W_TAX, W_YTD ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        try {
            Connection connection = ds.getConnection();
            PreparedStatement insertWarehouseStmt = connection.prepareStatement(insertWarehousesCmd);
            String line;
            String[] lineData;
            try {
                System.out.println("Start loading data for table : warehouses");
                FileReader fr = new FileReader("/temp/team_p/node1/extern/warehouse.csv");
                BufferedReader bf = new BufferedReader(fr);
                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");
                    insertWarehouseStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertWarehouseStmt.setString(2, lineData[1]);
                    insertWarehouseStmt.setString(3, lineData[2]);
                    insertWarehouseStmt.setString(4, lineData[3]);
                    insertWarehouseStmt.setString(5, lineData[4]);
                    insertWarehouseStmt.setString(6, lineData[5]);
                    insertWarehouseStmt.setString(7, lineData[6]);
                    insertWarehouseStmt.setBigDecimal(8, new BigDecimal(lineData[7]));
                    insertWarehouseStmt.setBigDecimal(9, new BigDecimal(lineData[8]));


                    insertWarehouseStmt.execute();
                }

                System.out.println("Successfully loaded all data for table : warehouses ");
            } catch (IOException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadDistricts() {
        String insertDistrictsCmd = "INSERT INTO " + " district ("
                + " D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, "
                + " D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID, D_NEXT_DELIVER_O_ID) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

        try {
            Connection connection = ds.getConnection();
            PreparedStatement insertByDistrictStmt = connection.prepareStatement(insertDistrictsCmd);
            String line;
            String[] lineData;
            FileReader fr;
            BufferedReader bf;
            //Map<Double<Integer, Integer>, Integer> orderMap = new HashMap<>();

            try {
                System.out.println("Start loading data for table : districts");
                fr = new FileReader("/temp/team_p/node1/extern/district-aug.csv");
                bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");

                    insertByDistrictStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertByDistrictStmt.setInt(2, Integer.parseInt(lineData[1]));
                    insertByDistrictStmt.setString(3, lineData[2]);
                    insertByDistrictStmt.setString(4, lineData[3]);
                    insertByDistrictStmt.setString(5, lineData[4]);
                    insertByDistrictStmt.setString(6, lineData[5]);
                    insertByDistrictStmt.setString(7, lineData[6]);
                    insertByDistrictStmt.setString(8, lineData[7]);
                    insertByDistrictStmt.setBigDecimal(9, new BigDecimal(lineData[8]));
                    insertByDistrictStmt.setBigDecimal(10, new BigDecimal(lineData[9]));
                    insertByDistrictStmt.setInt(11, Integer.parseInt(lineData[10]));
                    insertByDistrictStmt.setInt(12, Integer.parseInt(lineData[11]));

                    insertByDistrictStmt.execute();
                }

                System.out.println("Successfully loaded all data for table : districts ");
            } catch (IOException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void loadCustomer2() {
        String insertCustomerCmd = "IMPORT INTO " + " customer ("
                + " C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
                + " C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,"
                + " C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,"
                + " C_DATA, C_W_NAME, C_D_NAME) "
                + "    CSV DATA ("
                + "      'nodelocal:///customer-aug.csv'" +
                "    );";
        try {
            Connection connection = ds.getConnection();
            System.out.println("Start loading data for table : customer");
            connection.createStatement().executeQuery(insertCustomerCmd);
            System.out.println("Successfully loaded all data for table : customer ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadOrder2() {
        String insertOrderCmd = "IMPORT INTO " + " orders ("
                + " O_W_ID, O_D_ID, O_ID, "
                + " O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)"
                + "    CSV DATA ("
                + "      'nodelocal:///order.csv'" +
                "    )  WITH nullif = '';";
        try {
            Connection connection = ds.getConnection();
            System.out.println("Start loading data for table : orders");
            connection.createStatement().executeQuery(insertOrderCmd);
            System.out.println("Successfully loaded all data for table : orders ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadItem2() {
        String insertItemCmd = "IMPORT INTO " + " item ("
                + " I_ID, I_NAME, I_PRICE, "
                + " I_IM_ID, I_DATA)"
                + "    CSV DATA ("
                + "      'nodelocal:///item.csv'" +
                "    );";
        try {
            Connection connection = ds.getConnection();
            System.out.println("Start loading data for table : item");
            connection.createStatement().executeQuery(insertItemCmd);
            System.out.println("Successfully loaded all data for table : item ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadOrderLines2() {
        String insertOrderLineCmd = "IMPORT INTO " + " order_line ("
                + " OL_W_ID, OL_D_ID, OL_O_ID, "
                + " OL_NUMBER, OL_I_ID, OL_DELIVERY_D, "
                + " OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, "
                + " OL_DIST_INFO, OL_I_NAME) "
                + "    CSV DATA ("
                + "      'nodelocal:///order-line-aug.csv'" +
                "    ) WITH nullif = '';";
        try {
            Connection connection = ds.getConnection();
            System.out.println("Start loading data for table : order_line");
            connection.createStatement().executeQuery(insertOrderLineCmd);
            System.out.println("Successfully loaded all data for table : order_line ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadStock2() {
        String insertStockCmd = "IMPORT INTO " + " stock ("
                + " S_W_ID, S_I_ID, S_QUANTITY, "
                + " S_YTD, S_ORDER_CNT, S_REMOTE_CNT, "
                + " S_DIST_01, S_DIST_02, S_DIST_03, "
                + " S_DIST_04, S_DIST_05, S_DIST_06, "
                + " S_DIST_07, S_DIST_08, S_DIST_09, "
                + " S_DIST_10, S_DATA, S_I_NAME, "
                + " S_I_PRICE) "
                + "    CSV DATA ("
                + "      'nodelocal:///stock-aug.csv'" +
                "    );";
        try {
            Connection connection = ds.getConnection();
            System.out.println("Start loading data for table : stock");
            connection.createStatement().executeQuery(insertStockCmd);
            System.out.println("Successfully loaded all data for table : stock ");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    /*private void loadCustomer() {
        //String insertCustomerCmd = "INSERT INTO " + " customer ("
        //        + " C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
        //        + " C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,"
        //        + " C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT,"
        //        + " C_DATA, C_W_NAME, C_D_NAME) "
        //        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        //        + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        //        + " ?, ?, ?); ";
        //String insertOrdersCmd = "INSERT INTO " + " orders ("
        //        + " O_W_ID, O_D_ID, O_ID, "
        //        + " O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D"
        //        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";
        //String insertOrdersByIdCmd = "INSERT INTO " + " orders_by_id ("
        //        + " O_W_ID, O_D_ID, O_ID, O_C_ID, "
        //        + " O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, "
        //        + " O_C_FIRST, O_C_MIDDLE, O_C_LAST ) "
        //        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        try {
            Connection connection = ds.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement insertCustomerStmt = connection.prepareStatement(insertCustomerCmd);
            //PreparedStatement insertOrderByTimestampStmt = connection.prepareStatement(insertOrdersCmd);
            //PreparedStatement insertOrderByIdStmt = connection.prepareStatement(insertOrdersByIdCmd);
            FileReader fr;
            BufferedReader bf;
            String line;
            int BATCH_SIZE = 512;
            int BATCH_NUM = 0;
            //Set<Order> orderSet = new HashSet<>();
            //Map<Integer, Triple<Integer, Date, Integer>> orderMap = new HashMap<>();
            //Map<Integer, Triple<String, String, String>> customerMap = new HashMap<>();

            try {
                // load order data
                //System.out.println("Read data from order file.");
                //fr = new FileReader("project-files2/data-files/order.csv");
                //bf = new BufferedReader(fr);
                String[] lineData;
                //
                //while ((line = bf.readLine()) != null) {
                //    lineData = line.split(",");
                //
                //    // set <C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID> map
                //    int orderId = Integer.parseInt(lineData[2]);
                //    int customerId = Integer.parseInt(lineData[3]);
                //    Date entryDate = DF.parse(lineData[7]);
                //    int carrierId = -1; // default value -1 to indicate null
                //    if (!lineData[4].equals("null")) {
                //        carrierId = Integer.parseInt(lineData[4]);
                //    }
                //    if (!orderMap.containsKey(customerId)) {
                //        orderMap.put(customerId, new Triple<>(orderId, entryDate, carrierId));
                //    } else if (orderMap.get(customerId).first < orderId) {
                //        orderMap.put(customerId, new Triple<>(orderId, entryDate, carrierId));
                //    }
                //
                //    // save locally first, later load into DB
                //    Order curOrder = (new Order())
                //            .setWId(Integer.parseInt(lineData[0]))
                //            .setDId(Integer.parseInt(lineData[1]))
                //            .setEntryDate(entryDate)
                //            .setId(orderId)
                //            .setCId(customerId)
                //            .setCarrierId(carrierId)
                //            .setOlCnt(new BigDecimal(lineData[5]))
                //            .setAllLocal(new BigDecimal(lineData[6]));
                //    orderSet.add(curOrder);
                //}
                //System.out.println("Successfully loaded all data from order file.");

                // load customer
                System.out.println("Start loading data for table : customers");
                fr = new FileReader("project-files2/data-files/customer-aug.csv");
                bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");

                    int customerId = Integer.parseInt(lineData[2]);
                    String firstName = lineData[3];
                    String middleName = lineData[4];
                    String lastName = lineData[5];
                    //customerMap.put(customerId, new Triple<>(firstName, middleName, lastName));

                    // retrieve <C_LAST_ORDER, C_ENTRY_D, C_CARRIER_ID> triple
                    //Triple triple = orderMap.get(customerId);
                    insertCustomerStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertCustomerStmt.setInt(2, Integer.parseInt(lineData[1]));
                    insertCustomerStmt.setInt(3, customerId);
                    insertCustomerStmt.setString(4, firstName);
                    insertCustomerStmt.setString(5, middleName);
                    insertCustomerStmt.setString(6, lastName);
                    insertCustomerStmt.setString(7, lineData[6]);
                    insertCustomerStmt.setString(8, lineData[7]);
                    insertCustomerStmt.setString(9, lineData[8]);
                    insertCustomerStmt.setString(10, lineData[9]);
                    insertCustomerStmt.setString(11, lineData[10]);
                    insertCustomerStmt.setString(12, lineData[11]);
                    insertCustomerStmt.setTimestamp(13, new Timestamp(DF.parse(lineData[12]).getTime()));
                    insertCustomerStmt.setString(14, lineData[13]);
                    insertCustomerStmt.setBigDecimal(15, new BigDecimal(lineData[14]));
                    insertCustomerStmt.setBigDecimal(16, new BigDecimal(lineData[15]));
                    insertCustomerStmt.setBigDecimal(17, new BigDecimal(lineData[16]));
                    insertCustomerStmt.setFloat(18, Float.parseFloat(lineData[17]));
                    insertCustomerStmt.setInt(19, Integer.parseInt(lineData[18]));
                    insertCustomerStmt.setInt(20, Integer.parseInt(lineData[19]));
                    insertCustomerStmt.setString(21, lineData[20]);
                    insertCustomerStmt.setString(22, lineData[21]);
                    insertCustomerStmt.setString(23, lineData[22]);
                    //insertCustomerStmt.setInt(22, (Integer) triple.first);
                    //System.out.println("second before: "+triple.second.toString());
                    //Date test = triple.second.toString();
                    //DateFormat dateFormat;
                    //Date second = DF.parse(triple.second.toString());
                    //SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
                    //Date second = sdf.parse(triple.second.toString());
                    //insertCustomerStmt.setTimestamp(23, new Timestamp(second.getTime()));
                    ////System.out.println("second" + second);
                    //insertCustomerStmt.setInt(24, (Integer) triple.third);

                    if (BATCH_NUM < BATCH_SIZE) {
                        insertCustomerStmt.addBatch();
                        BATCH_NUM++;
                    } else {
                        BATCH_NUM = 0;
                        int[] count1 = insertCustomerStmt.executeBatch();
                        System.out.printf("    => %s row(s) insertCustomerStmt updated in this batch\n", count1.length);
                    }
                    //insertCustomerStmt.execute();
                }
                if (BATCH_NUM > 0) {
                    int[] count2 = insertCustomerStmt.executeBatch();
                    System.out.printf("    => %s row(s) insertCustomerStmt updated in this batch\n", count2.length);
                }


                connection.commit();
                //BATCH_NUM = 0;
                System.out.println("Successfully loaded all data for table : customers ");

                //// load order data into DB
                //System.out.println("Start loading data for table : orders_by_timestamp and orders_by_id");
                ////for (int i = 0; i <= orderSet.size() / BATCH_SIZE; i++) {
                //for (Order order : orderSet) {
                //    Triple triple = customerMap.get(order.cId);
                //
                //    insertOrderByTimestampStmt.setInt(1, order.wId);
                //    insertOrderByTimestampStmt.setInt(2, order.dId);
                //    insertOrderByTimestampStmt.setTimestamp(3, new Timestamp(order.entryDate.getTime()));
                //    insertOrderByTimestampStmt.setInt(4, order.id);
                //    insertOrderByTimestampStmt.setInt(5, order.cId);
                //    insertOrderByTimestampStmt.setInt(6, order.carrierId);
                //    insertOrderByTimestampStmt.setBigDecimal(7, order.olCnt);
                //    insertOrderByTimestampStmt.setBigDecimal(8, order.allLocal);
                //    insertOrderByTimestampStmt.setString(9, (String) triple.first);
                //    insertOrderByTimestampStmt.setString(10, (String) triple.second);
                //    insertOrderByTimestampStmt.setString(11, (String) triple.third);
                //
                //    insertOrderByIdStmt.setInt(1, order.wId);
                //    insertOrderByIdStmt.setInt(2, order.dId);
                //    insertOrderByIdStmt.setInt(3, order.id);
                //    insertOrderByIdStmt.setInt(4, order.cId);
                //    insertOrderByIdStmt.setTimestamp(5, new Timestamp(order.entryDate.getTime()));
                //    insertOrderByIdStmt.setInt(6, order.carrierId);
                //    insertOrderByIdStmt.setBigDecimal(7, order.olCnt);
                //    insertOrderByIdStmt.setBigDecimal(8, order.allLocal);
                //    insertOrderByIdStmt.setString(9, (String) triple.first);
                //    insertOrderByIdStmt.setString(10, (String) triple.second);
                //    insertOrderByIdStmt.setString(11, (String) triple.third);
                //
                //    //BoundStatement boundByTimestamp = insertOrderByTimestampStmt.bind(
                //    //        order.wId, order.dId, order.entryDate, order.id,
                //    //        order.cId, order.carrierId, order.olCnt, order.allLocal,
                //    //        triple.first, triple.second, triple.third);
                //    //BoundStatement boundById = insertOrderByIdStmt.bind(
                //    //        order.wId, order.dId, order.id, order.cId,
                //    //        order.entryDate, order.carrierId, order.olCnt, order.allLocal,
                //    //        triple.first, triple.second, triple.third);
                //    if (BATCH_NUM < BATCH_SIZE) {
                //        insertOrderByTimestampStmt.addBatch();
                //        insertOrderByIdStmt.addBatch();
                //        BATCH_NUM++;
                //    } else {
                //        BATCH_NUM = 0;
                //        int[] count3 = insertOrderByTimestampStmt.executeBatch();
                //        int[] count4 = insertOrderByIdStmt.executeBatch();
                //        System.out.printf("    => %s insertOrderByTimestampStmt row(s) updated in this batch\n", count3.length);
                //        System.out.printf("    => %s insertOrderByIdStmt row(s) updated in this batch\n", count4.length);
                //        //insertOrderByTimestampStmt.executeBatch();
                //        //insertOrderByIdStmt.executeBatch();
                //    }
                //    //insertOrderByTimestampStmt.execute();
                //    //insertOrderByIdStmt.execute();
                //}
                //if (BATCH_NUM > 0) {
                //    int[] count5 = insertOrderByTimestampStmt.executeBatch();
                //    int[] count6 = insertOrderByIdStmt.executeBatch();
                //    System.out.printf("    => %s insertOrderByTimestampStmt row(s) updated in this batch\n", count5.length);
                //    System.out.printf("    => %s insertOrderByIdStmt row(s) updated in this batch\n", count6.length);
                //}
                //
                ////insertOrderByTimestampStmt.executeBatch();
                ////insertOrderByIdStmt.executeBatch();
                //connection.commit();
                //System.out.println("Successfully loaded all data for table : orders_by_timestamp and orders_by_id ");
            } catch (IOException | ParseException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            //System.out.printf("BasicExampleDAO.bulkInsertRandomAccountData ERROR: { state => %s, cause => %s, message => %s }\n",
            //        e.getSQLState(), e.getCause(), e.getMessage());
            e.printStackTrace();
            e.getNextException();
        }

    }*/

    /*private void loadOrders() {
        String insertOrdersCmd = "INSERT INTO " + " orders ("
                + " O_W_ID, O_D_ID, O_ID, "
                + " O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";
        try {
            Connection connection = ds.getConnection();
            PreparedStatement insertByDistrictStmt = connection.prepareStatement(insertOrdersCmd);
            String line;
            String[] lineData;
            try {
                System.out.println("Start loading data for table : orders");
                FileReader fr = new FileReader("project-files2/data-files/orders.csv");
                BufferedReader bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");

                    insertByDistrictStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertByDistrictStmt.setInt(2, Integer.parseInt(lineData[1]));
                    insertByDistrictStmt.setString(3, lineData[2]);
                    insertByDistrictStmt.setString(4, lineData[3]);
                    insertByDistrictStmt.setString(5, lineData[4]);
                    insertByDistrictStmt.setString(6, lineData[5]);
                    insertByDistrictStmt.setString(7, lineData[6]);
                    insertByDistrictStmt.setString(8, lineData[7]);
                    insertByDistrictStmt.setBigDecimal(9, new BigDecimal(lineData[8]));
                    insertByDistrictStmt.setBigDecimal(10, new BigDecimal(lineData[9]));
                    insertByDistrictStmt.setInt(11, Integer.parseInt(lineData[10]));

                    insertByDistrictStmt.execute();
                }

                System.out.println("Successfully loaded all data for table : districts ");
            } catch (IOException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }*/
    /*private void loadItemsAndOrderLines() {
        String insertOrderLinesCmd = "INSERT INTO " + " order_lines ("
                + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_I_NAME, "
                + " OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        String insertItemsCmd = "INSERT INTO " + " items ("
                + " I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA ) "
                + " VALUES (?, ?, ?, ?, ?); ";
        try {
            Connection connection = ds.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement insertOrderLinesStmt = connection.prepareStatement(insertOrderLinesCmd);
            PreparedStatement insertItemStmt = connection.prepareStatement(insertItemsCmd);

            FileReader fr;
            BufferedReader bf;
            String line;
            String[] lineData;
            int BATCH_SIZE = 256;
            int BATCH_NUM = 0;
            Map<Integer, String> hm = new HashMap<>();

            try {
                System.out.println("Start loading data for table : items");
                fr = new FileReader("project-files2/data-files/item.csv");
                bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");
                    int itemId = Integer.parseInt(lineData[0]);
                    String itemName = lineData[1];
                    hm.put(itemId, itemName);
                    insertItemStmt.setInt(1, itemId);
                    insertItemStmt.setString(2, itemName);
                    insertItemStmt.setBigDecimal(3, new BigDecimal(lineData[2]));
                    insertItemStmt.setInt(4, Integer.parseInt(lineData[3]));
                    insertItemStmt.setString(5, lineData[4]);

                    if (BATCH_NUM < BATCH_SIZE) {
                        insertItemStmt.addBatch();
                        BATCH_NUM++;
                    } else {
                        BATCH_NUM = 0;
                        int[] count1 = insertItemStmt.executeBatch();
                        System.out.printf("    => %s row(s) insertItemStmt updated in this batch\n", count1.length);
                    }
                    //BoundStatement bound = insertItemStmt.bind(
                    //        itemId, itemName, new BigDecimal(lineData[2]),
                    //        Integer.parseInt(lineData[3]), lineData[4]);
                    //insertItemStmt.execute();
                }
                if (BATCH_NUM > 0) {
                    int[] count2 = insertItemStmt.executeBatch();
                    System.out.printf("    => %s row(s) insertItemStmt updated in this batch\n", count2.length);
                }

                BATCH_NUM = 0;
                System.out.println("Successfully loaded all data for table : items ");

                System.out.println("Start loading data for table : order_lines");
                fr = new FileReader("project-files2/data-files/order-line.csv");
                bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");
                    Date date;
                    Timestamp ts;
                    if (lineData[5].equals("null")) {
                        //date = null;
                        ts = null;
                    } else {
                        date = DF.parse(lineData[5]);
                        ts = new Timestamp(date.getTime());
                    }
                    int itemId = Integer.parseInt(lineData[4]);
                    insertOrderLinesStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertOrderLinesStmt.setInt(2, Integer.parseInt(lineData[1]));
                    insertOrderLinesStmt.setInt(3, Integer.parseInt(lineData[2]));
                    insertOrderLinesStmt.setInt(4, Integer.parseInt(lineData[3]));
                    insertOrderLinesStmt.setInt(5, itemId);
                    insertOrderLinesStmt.setString(6, hm.get(itemId));
                    insertOrderLinesStmt.setTimestamp(7, ts);
                    insertOrderLinesStmt.setBigDecimal(8, new BigDecimal(lineData[6]));
                    insertOrderLinesStmt.setInt(9, Integer.parseInt(lineData[7]));
                    insertOrderLinesStmt.setBigDecimal(10, new BigDecimal(lineData[8]));
                    insertOrderLinesStmt.setString(11, lineData[9]);
                    if (BATCH_NUM < BATCH_SIZE) {
                        insertOrderLinesStmt.addBatch();
                        BATCH_NUM++;
                    } else {
                        BATCH_NUM = 0;
                        int[] count3 = insertOrderLinesStmt.executeBatch();
                        System.out.printf("    => %s row(s) insertOrderLinesStmt updated in this batch\n", count3.length);
                    }
                    //insertOrderLinesStmt.execute();
                }
                if (BATCH_NUM > 0) {
                    int[] count4 = insertOrderLinesStmt.executeBatch();
                    System.out.printf("    => %s row(s) insertOrderLinesStmt updated in this batch\n", count4.length);
                }


                connection.commit();
                System.out.println("Successfully loaded all data for table : order_lines ");
            } catch (IOException | ParseException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.printf("BasicExampleDAO.bulkInsertRandomAccountData ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }*/

    /*private void loadStock() {
        String insertStocksCmd = "INSERT INTO " + " stocks ("
                + " S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, "
                + " S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                + " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

        try {
            Connection connection = ds.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement insertStockStmt = connection.prepareStatement(insertStocksCmd);
            String line;
            String[] lineData;
            int BATCH_SIZE = 256;
            int BATCH_NUM = 0;

            try {
                System.out.println("Start loading data for table : stocks");
                FileReader fr = new FileReader("project-files2/data-files/stock.csv");
                BufferedReader bf = new BufferedReader(fr);

                while ((line = bf.readLine()) != null) {
                    lineData = line.split(",");
                    insertStockStmt.setInt(1, Integer.parseInt(lineData[0]));
                    insertStockStmt.setInt(2, Integer.parseInt(lineData[1]));
                    insertStockStmt.setBigDecimal(3, new BigDecimal(lineData[2]));
                    insertStockStmt.setBigDecimal(4, new BigDecimal(lineData[3]));
                    insertStockStmt.setInt(5, Integer.parseInt(lineData[4]));
                    insertStockStmt.setInt(6, Integer.parseInt(lineData[5]));
                    insertStockStmt.setString(7, lineData[6]);
                    insertStockStmt.setString(8, lineData[7]);
                    insertStockStmt.setString(9, lineData[8]);
                    insertStockStmt.setString(10, lineData[9]);
                    insertStockStmt.setString(11, lineData[10]);
                    insertStockStmt.setString(12, lineData[11]);
                    insertStockStmt.setString(13, lineData[12]);
                    insertStockStmt.setString(14, lineData[13]);
                    insertStockStmt.setString(15, lineData[14]);
                    insertStockStmt.setString(16, lineData[15]);
                    insertStockStmt.setString(17, lineData[16]);
                    if (BATCH_NUM < BATCH_SIZE) {
                        insertStockStmt.addBatch();
                        BATCH_NUM++;
                    } else {
                        BATCH_NUM = 0;
                        int[] count1 = insertStockStmt.executeBatch();
                        System.out.printf("    => %s row(s) insertStockStmt updated in this batch\n", count1.length);
                    }
                    //insertStockStmt.execute();
                }
                if (BATCH_NUM > 0) {
                    int[] count2 = insertStockStmt.executeBatch();
                    System.out.printf("    => %s row(s) insertStockStmt updated in this batch\n", count2.length);
                }

                connection.commit();
                System.out.println("Successfully loaded all data for table : stocks ");
            } catch (IOException e) {
                System.out.println("Load data failed with error : " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.printf("BasicExampleDAO.bulkInsertRandomAccountData ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }*/


    public void dropSchema() {
        runSQL("DROP VIEW IF EXISTS customer_sort_by_balance;");
        runSQL("DROP VIEW IF EXISTS order_sort_by_customer;");
        runSQL("DROP VIEW IF EXISTS order_line_by_item;");
        runSQL("DROP TABLE IF EXISTS warehouse;");
        runSQL("DROP TABLE IF EXISTS district;");
        runSQL("DROP TABLE IF EXISTS customer;");
        runSQL("DROP TABLE IF EXISTS orders;");
        runSQL("DROP TABLE IF EXISTS item;");
        runSQL("DROP TABLE IF EXISTS order_line;");
        runSQL("DROP TABLE IF EXISTS stock;");

        //runSQL("DROP VIEW IF EXISTS customers_balances;");
        //runSQL("DROP TABLE IF EXISTS warehouses;");
        //runSQL("DROP TABLE IF EXISTS districts;");
        //runSQL("DROP TABLE IF EXISTS customers;");
        //runSQL("DROP TABLE IF EXISTS orders_by_timestamp;");
        //runSQL("DROP TABLE IF EXISTS orders_by_id;");
        //runSQL("DROP TABLE IF EXISTS items;");
        //runSQL("DROP TABLE IF EXISTS order_lines;");
        //runSQL("DROP TABLE IF EXISTS stocks;");

    }
}

//class Order {
//    int wId;
//    int dId;
//    Date entryDate;
//    int id;
//    int cId;
//    int carrierId = -1; // indicate not valid
//    BigDecimal olCnt;
//    BigDecimal allLocal;
//    String cFirst;
//    String cMiddle;
//    String cLast;
//
//    Order() {
//    }
//
//    Order setWId(int wId) {
//        this.wId = wId;
//        return this;
//    }
//
//    Order setDId(int dId) {
//        this.dId = dId;
//        return this;
//    }
//
//    Order setEntryDate(Date entryDate) {
//        this.entryDate = entryDate;
//        return this;
//    }
//
//    Order setId(int id) {
//        this.id = id;
//        return this;
//    }
//
//    Order setCId(int cId) {
//        this.cId = cId;
//        return this;
//    }
//
//    Order setCarrierId(int carrierId) {
//        this.carrierId = carrierId;
//        return this;
//    }
//
//    Order setOlCnt(BigDecimal olCnt) {
//        this.olCnt = olCnt;
//        return this;
//    }
//
//    Order setAllLocal(BigDecimal allLocal) {
//        this.allLocal = allLocal;
//        return this;
//    }
//
//    Order setCFirst(String cFirst) {
//        this.cFirst = cFirst;
//        return this;
//    }
//
//    Order setCMiddle(String cMiddle) {
//        this.cMiddle = cMiddle;
//        return this;
//    }
//
//    Order setCLast(String cLast) {
//        this.cLast = cLast;
//        return this;
//    }
//}

/*
class Triple<T, U, V> {
    final T first;
    final U second;
    final V third;

    Triple(T first, U second, V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}*/
//class Double<T, U> {
//    final T first;
//    final U second;
//
//
//    Double(T first, U second) {
//        this.first = first;
//        this.second = second;
//    }
//
//    public T getFirst() {
//        return first;
//    }
//
//    public U getSecond() {
//        return second;
//    }
//}