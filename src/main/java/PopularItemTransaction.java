
import com.sun.rowset.internal.Row;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PopularItemTransaction {
    private PreparedStatement selectLastOrdersStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement selectTopQuantityStmt;
    private PreparedStatement selectOrderWithItemStmt;

    public Connection connection;

    private static final String SELECT_LAST_ORDERS =
            "SELECT o_id, o_c_id, o_entry_d "
                    + "FROM orders "
                    + "WHERE o_w_id = ? AND o_d_id = ? "
                    + "LIMIT ? ;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_first, c_middle, c_last "
                    + "FROM customer "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ;";
    private static final String SELECT_TOP_QUANTITY =
            "SELECT ol_quantity "
                    + "FROM order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? LIMIT 1 ;";
    private static final String SELECT_ORDER_WITH_ITEM =
            "SELECT ol_i_id, ol_i_name, ol_quantity "
                    + "FROM order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ? ;";

    PopularItemTransaction(Connection connection) {
        this.connection = connection;
        try {
            selectLastOrdersStmt = connection.prepareStatement(SELECT_LAST_ORDERS);
            selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
            selectTopQuantityStmt = connection.prepareStatement(SELECT_TOP_QUANTITY);
            selectOrderWithItemStmt = connection.prepareStatement(SELECT_ORDER_WITH_ITEM);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    void popularItem(int wId, int dId, int numOfOrders) {
        ResultSet lastOrders = selectLastOrders(wId, dId, numOfOrders);
        HashMap<Integer, Doubles<String, Integer>> popularItems = new HashMap<>();
        ResultSet popularItem = null;
        ResultSet custom = null;
        int order_cnt = 0;

        System.out.println("District Identifier WId: " + wId + ",DId: " + dId);
        System.out.println("number of orders been examined: " + numOfOrders);
        try {
            while (lastOrders.next()) {
                int orderId = lastOrders.getInt("o_id");
                int orderCId = lastOrders.getInt("o_c_id");
                String orderEntryDay = lastOrders.getTimestamp("o_entry_d").toString();
                System.out.println("order Id: " + orderId + ", entry date and time: " + orderEntryDay);

                popularItem = getPopularItem(wId, dId, orderId);
                custom = getCustomer(wId, dId, orderCId);
                custom.next();

                String c_first = custom.getString("c_first");
                String c_middle = custom.getString("c_middle");
                String c_last = custom.getString("c_last");
                System.out.println("customer name: " + c_first + " " + c_middle + " " + c_last);

                System.out.println("============ popular item info ============");
                while (popularItem.next()) {
                    int itemId = popularItem.getInt("ol_i_id");
                    String itemName = popularItem.getString("ol_i_name");
                    float itemQuantity = popularItem.getBigDecimal("ol_quantity").floatValue();
                    System.out.println("item name: " + itemName);
                    System.out.println("item quantity: " + itemQuantity);


                    if (!popularItems.containsKey(itemId)) {
                        Doubles<String, Integer> itemDouble = new Doubles<>(itemName, 1);
                        //System.out.println("second when add key:" + itemDouble.second);
                        popularItems.put(itemId, itemDouble);
                        //System.out.println("Put successfully");
                    } else {
                        int newItemCnt = popularItems.get(itemId).getSecond() + 1;
                        Doubles<String, Integer> newItemDouble = new Doubles<>(itemName, newItemCnt);
                        //newItemDouble.put(itemName, newItemCnt);
                        //System.out.println("second when exist key:" + newItemDouble.second);
                        popularItems.replace(itemId, newItemDouble);
                    }
                }
                System.out.println("==========================================");

                order_cnt = lastOrders.getRow();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("============ percentage info ============");
        for (Map.Entry<Integer, Doubles<String, Integer>> entry : popularItems.entrySet()) {
            //System.out.println("before itemINfo");
            Doubles<String, Integer> itemInfo = entry.getValue();
            //System.out.println("print itemINfo" +itemINfo);
            String itemNameInfo = itemInfo.getFirst();
            int itemNum = itemInfo.getSecond();
            //System.out.println("second:"+itemNum+" & order_cnt:"+order_cnt);
            //float percentage = itemInfo.getSecond() / order_cnt;
            System.out.println("item name: " + itemNameInfo);
            System.out.println(String.format("percentage: %.3f", (float) itemNum / order_cnt));

        }
        System.out.println("==========================================");
    }


    private ResultSet selectLastOrders(final int wId, final int dId, final int numOfOrders) {
        ResultSet resultSet = null;
        try {
            selectLastOrdersStmt.setInt(1, wId);
            selectLastOrdersStmt.setInt(2, dId);
            selectLastOrdersStmt.setInt(3, numOfOrders);

            selectLastOrdersStmt.execute();
            resultSet = selectLastOrdersStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }


    private ResultSet getCustomer(final int wId, final int dId, final int orderCId) {
        ResultSet resultSet = null;
        try {
            selectCustomerStmt.setInt(1, wId);
            selectCustomerStmt.setInt(2, dId);
            selectCustomerStmt.setInt(3, orderCId);
            selectCustomerStmt.execute();
            resultSet = selectCustomerStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    private ResultSet getPopularItem(final int wId, final int dId, final int orderId) {
        ResultSet resultSet2 = null;
        try {
            selectTopQuantityStmt.setInt(1, wId);
            selectTopQuantityStmt.setInt(2, dId);
            selectTopQuantityStmt.setInt(3, orderId);
            selectTopQuantityStmt.execute();
            ResultSet resultSet1 = selectTopQuantityStmt.getResultSet();
            resultSet1.next();

            BigDecimal maxQuantity = resultSet1.getBigDecimal("ol_quantity");

            selectOrderWithItemStmt.setInt(1, wId);
            selectOrderWithItemStmt.setInt(2, dId);
            selectOrderWithItemStmt.setInt(3, orderId);
            selectOrderWithItemStmt.setBigDecimal(4, maxQuantity);
            selectOrderWithItemStmt.execute();
            resultSet2 = selectOrderWithItemStmt.getResultSet();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet2;
    }


    class Doubles<T, U> {
        final T first;
        final U second;


        Doubles(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }
}

