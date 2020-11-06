import org.postgresql.ds.PGSimpleDataSource;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.List;

class OrderTransaction {
    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement selectStockStmt;
    private PreparedStatement updateDistrictNextOIdStmt;
    private PreparedStatement updateStockStmt;
    private PreparedStatement insertOrderStmt;
    private PreparedStatement insertOrderLineStmt;
    public Connection connection;


    private static final String SELECT_DISTRICT =
            " SELECT D_NEXT_O_ID, D_TAX "
                    + " FROM district "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String UPDATE_DISTRICT_NEXT_O_ID =
            " UPDATE district "
                    + " SET D_NEXT_O_ID = ? "
                    + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_WAREHOUSE =
            " SELECT W_TAX "
                    + " FROM warehouse "
                    + " WHERE W_ID = ?; ";
    private static final String SELECT_CUSTOMER =
            " SELECT C_W_ID, C_D_ID, C_ID, C_LAST, C_CREDIT, C_DISCOUNT "
                    + " FROM customer "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String INSERT_ORDER =
            " INSERT INTO orders ("
                    + " O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) "
                    + " VALUES (?, ?, ?, ?, ?, ?, ?); ";
    private static final String SELECT_STOCK =
            " SELECT S_I_NAME, S_I_PRICE, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT "
                    + " FROM stock "
                    + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String UPDATE_STOCK =
            " UPDATE stock "
                    + " SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? "
                    + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String INSERT_ORDER_LINE =
            "INSERT INTO order_line ("
                    + " OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, OL_I_NAME )"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

    OrderTransaction(Connection connection) {
        this.connection = connection;
        try {
            this.selectWarehouseStmt = connection.prepareStatement(SELECT_WAREHOUSE);
            this.selectDistrictStmt = connection.prepareStatement(SELECT_DISTRICT);
            this.selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
            this.selectStockStmt = connection.prepareStatement(SELECT_STOCK);
            this.updateDistrictNextOIdStmt = connection.prepareStatement(UPDATE_DISTRICT_NEXT_O_ID);
            this.updateStockStmt = connection.prepareStatement(UPDATE_STOCK);
            this.insertOrderStmt = connection.prepareStatement(INSERT_ORDER);
            this.insertOrderLineStmt = connection.prepareStatement(INSERT_ORDER_LINE);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    void processOrder(int cId, int wId, int dId, List<List<Integer>> itemOrders) {
        ResultSet district = getDistrict(wId, dId);
        ResultSet customer = getCustomer(wId, dId, cId);
        ResultSet warehouse = getWarehouse(wId);

        try {
            district.next();
            warehouse.next();
            customer.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        double dTax = 0;
        double wTax = 0;
        int nextOId = 0;
        try {
            nextOId = district.getInt("D_NEXT_O_ID");
            dTax = district.getBigDecimal("D_TAX").doubleValue();
            wTax = warehouse.getBigDecimal("W_TAX").doubleValue();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        updateDistrictNextOId(nextOId + 1, wId, dId);

        Date curDate = new Date();
        BigDecimal olCount = new BigDecimal(itemOrders.size());
        BigDecimal allLocal = new BigDecimal(1);
        for (List<Integer> order : itemOrders) {
            if (order.get(1) != wId) {
                allLocal = new BigDecimal(0);
            }
        }

        createNewOrder(nextOId, dId, wId, cId, curDate, olCount, allLocal);

        double totalAmount = 0;
        for (int i = 0; i < itemOrders.size(); i++) {
            int iId = itemOrders.get(i).get(0);
            int iWId = itemOrders.get(i).get(1);
            int quantity = itemOrders.get(i).get(2);

            ResultSet stock = selectStock(iWId, iId);

            double adjQuantity = 0;
            try {
                stock.next();
                adjQuantity = stock.getBigDecimal("S_QUANTITY").doubleValue() - quantity;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            while (adjQuantity < 10) {
                adjQuantity += 100;
            }
            BigDecimal adjQuantityDecimal = new BigDecimal(adjQuantity);

            try {
                updateStock(iWId, iId, adjQuantityDecimal,
                        stock.getBigDecimal("S_YTD").add(new BigDecimal(quantity)),
                        stock.getInt("S_ORDER_CNT") + 1,
                        (iWId != wId)
                                ? stock.getInt("S_REMOTE_CNT") + 1
                                : stock.getInt("S_REMOTE_CNT"));
            } catch (SQLException e) {
                e.printStackTrace();
            }

            //ResultSet item = selectItem(iId);
            String itemName = null;
            BigDecimal itemAmount = null;
            try {
                itemName = stock.getString("S_I_NAME");
                itemAmount = stock.getBigDecimal("S_I_PRICE").multiply(new BigDecimal(quantity));
                totalAmount += itemAmount.doubleValue();
                createNewOrderLine(wId, dId, nextOId, i, iId,
                        itemAmount, iWId, new BigDecimal(quantity), getDistrictStringId(dId), itemName);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            System.out.printf(
                    "itemId: %d, itemName: %s, warehouseId: %d, quantity: %d, OL_AMOUNT: %f, S_QUANTITY: %f \n",
                    iId, itemName, iWId, quantity, itemAmount, adjQuantity);
        }

        try {
            totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getBigDecimal("C_DISCOUNT").doubleValue());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // log
        try {
            System.out.printf(
                    "customer with C_W_ID: %d, C_D_ID: %d, C_ID: %d, C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %s \n",
                    wId, dId, cId, customer.getString("C_LAST"),
                    customer.getString("C_CREDIT"), customer.getBigDecimal("C_DISCOUNT"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.printf("Warehouse tax rate: %f, District tax rate: %f \n", wTax, dTax);
        System.out.printf("Order number: %d, entry date: %s \n", nextOId, curDate);
        System.out.printf("Number of items: %d, total amount for order: %f ", olCount.intValue(), totalAmount);
    }

    private void createNewOrderLine(int wId, int dId, int oId, int olNumber, int iId,
                                    BigDecimal itemAmount, int supplyWId, BigDecimal quantity, String distInfo, String itemName) {
        try {
            insertOrderLineStmt.setInt(1, oId);
            insertOrderLineStmt.setInt(2, dId);
            insertOrderLineStmt.setInt(3, wId);
            insertOrderLineStmt.setInt(4, olNumber);
            insertOrderLineStmt.setInt(5, iId);
            insertOrderLineStmt.setInt(6, supplyWId);
            insertOrderLineStmt.setBigDecimal(7, quantity);
            insertOrderLineStmt.setBigDecimal(8, itemAmount);
            insertOrderLineStmt.setString(9, distInfo);
            insertOrderLineStmt.setString(10, itemName);
            insertOrderLineStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //session.execute(insertOrderLineStmt.bind(
        //        wId, dId, oId, olNumber, iId, iName, itemAmount, supplyWId, quantity, distInfo));
    }

    private String getDistrictStringId(int dId) {
        if (dId < 10) {
            return "S_DIST_0" + dId;
        } else {
            return "S_DIST_10";
        }
    }


    private void updateStock(Integer wId, Integer iId, BigDecimal adjQuantity,
                             BigDecimal ytd, int orderCount, int remoteCount) {
        try {
            updateStockStmt.setBigDecimal(1, adjQuantity);
            updateStockStmt.setBigDecimal(2, ytd);
            updateStockStmt.setInt(3, orderCount);
            updateStockStmt.setInt(4, remoteCount);
            updateStockStmt.setInt(5, wId);
            updateStockStmt.setInt(6, iId);
            updateStockStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private ResultSet selectStock(Integer wId, Integer iId) {

        ResultSet resultSet = null;
        try {
            selectStockStmt.setInt(1, wId);
            selectStockStmt.setInt(2, iId);
            selectStockStmt.execute();
            resultSet = selectStockStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;


    }


    private void createNewOrder(int Oid, int dId, int wId, int cId, Date entryDate, BigDecimal olCount, BigDecimal
            allLocal) {
        try {
            insertOrderStmt.setInt(1, Oid);
            insertOrderStmt.setInt(2, dId);
            insertOrderStmt.setInt(3, wId);
            insertOrderStmt.setInt(4, cId);
            insertOrderStmt.setTimestamp(5, new Timestamp(entryDate.getTime()));
            insertOrderStmt.setBigDecimal(6, olCount);
            insertOrderStmt.setBigDecimal(7, allLocal);
            insertOrderStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void updateDistrictNextOId(int nextOId, int wId, int dId) {
        try {
            updateDistrictNextOIdStmt.setInt(1, nextOId);
            updateDistrictNextOIdStmt.setInt(2, wId);
            updateDistrictNextOIdStmt.setInt(3, dId);
            updateDistrictNextOIdStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private ResultSet getCustomer(int wId, int dID, int cId) {

        ResultSet resultSet = null;
        try {
            selectCustomerStmt.setInt(1, wId);
            selectCustomerStmt.setInt(2, dID);
            selectCustomerStmt.setInt(3, cId);
            selectCustomerStmt.execute();
            resultSet = selectCustomerStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    private ResultSet getWarehouse(int wId) {
        ResultSet resultSet = null;
        try {
            selectWarehouseStmt.setInt(1, wId);
            selectWarehouseStmt.execute();
            resultSet = selectWarehouseStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;


    }

    private ResultSet getDistrict(int wId, int dId) {
        ResultSet resultSet = null;
        try {
            selectDistrictStmt.setInt(1, wId);
            selectDistrictStmt.setInt(2, dId);
            selectDistrictStmt.execute();
            resultSet = selectDistrictStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;

    }

    //public static void main(String[] args) {
    //    PGSimpleDataSource ds = new PGSimpleDataSource();
    //    ds.setServerName("192.168.220.112");
    //    ds.setPortNumber(26257);
    //    ds.setDatabaseName("project");
    //    ds.setUser("test");
    //    ds.setPassword(null);
    //    //OrderTransaction orderTransaction = null;
    //    //PaymentTransaction paymentTransaction = null;
    //    //OrderStatusTransaction orderStatusTransaction = null;
    //    //StockLevelTransaction stockLevelTransaction = null;
    //    //DeliveryTransaction deliveryTransaction = null;
    //    //PopularItemTransaction popularItemTransaction = null;
    //    //TopBalanceTransaction topBalanceTransaction = null;
    //    RelatedCustomerTransaction relatedCustomerTransaction = null;
    //    try {
    //        //orderTransaction = new OrderTransaction(ds.getConnection());
    //        //paymentTransaction = new PaymentTransaction(ds.getConnection());
    //        //stockLevelTransaction = new StockLevelTransaction(ds.getConnection());
    //        //deliveryTransaction = new DeliveryTransaction(ds.getConnection());
    //        //popularItemTransaction = new PopularItemTransaction(ds.getConnection());
    //        //topBalanceTransaction = new TopBalanceTransaction(ds.getConnection());
    //        relatedCustomerTransaction = new RelatedCustomerTransaction(ds.getConnection());
    //    } catch (SQLException e) {
    //        e.printStackTrace();
    //    }
    //
    //    //String[] test = new String[]{
    //    //        "68195,1,1" ,
    //    //        "26567,1,5",
    //    //        "4114,1,7",
    //    //        "69343,1,3"};
    //    ////int itemCount = 4;
    //    //List<List<Integer>> itemOrders = new ArrayList<>();
    //    ////List<Integer> order1 = new ArrayList<>();
    //    //for (int i = 0; i < test.length; i++) {
    //    //    String[] orderString = test[i].split(",");
    //    //    List<Integer> order = new ArrayList<>();
    //    //    for (String s : orderString) {
    //    //        order.add(Integer.parseInt(s));
    //    //    }
    //    //    itemOrders.add(order);
    //    //}
    //
    //    //paymentTransaction.processPayment(1,2,30, (float) 723.94);
    //    //paymentTransaction.processPayment(1,2,894, (float) 1111.01);
    //
    //    //orderStatusTransaction.processOrderStatus(1, 1, 2207);
    //    //orderStatusTransaction.processOrderStatus(1, 1, 795);
    //    //stockLevelTransaction.processStockLevel(1,2,12,43);
    //    //stockLevelTransaction.processStockLevel(1,1,13,43);
    //    //deliveryTransaction.processDelivery(1, 10);
    //    //deliveryTransaction.processDelivery(1, 9);
    //    //popularItemTransaction.popularItem(1, 2, 43);
    //    //topBalanceTransaction.topBalance();
    //    relatedCustomerTransaction.processRelatedCustomer(1,1,2684);
    //}
}
