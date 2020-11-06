//import java.sql.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Set;
//
//public class RelatedCustomerTransaction {
//    private PreparedStatement selectCustomerStmt;
//    private PreparedStatement selectOrderLineStmt;
//    private PreparedStatement selectOrderLineByItemStmt;
//    private PreparedStatement selectCustomerByOrderStmt;
//    public Connection connection;
//
//    private static final String SELECT_CUSTOMER =
//            "SELECT o_id "
//                    + "FROM order_sort_by_customer "
//                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?;";
//    private static final String SELECT_ORDER_LINE =
//            "SELECT ol_i_id "
//                    + "FROM order_line "
//                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
//    private static final String SELECT_ORDER_LINE_BY_ITEM =
//            "SELECT ol_w_id, ol_d_id, ol_o_id, ol_i_id "
//                    + "FROM order_line_by_item "
//                    + "WHERE ol_w_id IN ? AND ol_d_id IN (1,2,3,4,5,6,7,8,9,10) AND ol_i_id IN ?;";
//    private static final String SELECT_CUSTOMER_BY_ORDER =
//            "SELECT o_c_id "
//                    + "FROM orders "
//                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;";
//
//    RelatedCustomerTransaction(Connection connection) {
//        this.connection = connection;
//        try {
//            this.selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
//            this.selectOrderLineStmt = connection.prepareStatement(SELECT_ORDER_LINE);
//            this.selectOrderLineByItemStmt = connection.prepareStatement(SELECT_ORDER_LINE_BY_ITEM);
//            this.selectCustomerByOrderStmt = connection.prepareStatement(SELECT_CUSTOMER_BY_ORDER);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    void processRelatedCustomer(int w_ID, int d_ID, int c_ID) {
//        Object[] wIdArray;
//        Object[] iIdArray;
//        customers_identifier ci = new customers_identifier(w_ID, d_ID, c_ID);
//        List<Integer> wArray = new ArrayList<>();
//        List<Integer> iArray = new ArrayList<>();
//        for (int i = 1; i < 11; i++) {
//            if (w_ID == i) {
//            } else {
//                wArray.add(i);
//            }
//        }
//        wIdArray = wArray.toArray();
//        Array Warray = connection.createArrayOf("Integer", wIdArray);
//
//        selectCustomerStmt.setInt(1, w_ID);
//        selectCustomerStmt.setInt(2, d_ID);
//        selectCustomerStmt.setInt(3, c_ID);
//        selectCustomerStmt.execute();
//        ResultSet orders = selectCustomerStmt.getResultSet();
//        while (orders.next()) {
//            int o_ID = orders.getInt("o_id");
//            selectOrderLineStmt.setInt(1, w_ID);
//            selectOrderLineStmt.setInt(2, d_ID);
//            selectOrderLineStmt.setInt(3, o_ID);
//            selectOrderLineStmt.execute();
//            ResultSet i_ids = selectOrderLineStmt.getResultSet();
//            while (i_ids.next()) {
//                int i_id = i_ids.getInt("ol_i_id");
//                iArray.add(i_id);
//                //selectOrderLineByItemStmt.setArray(1, );
//            }
//            iIdArray = iArray.toArray();
//            Array iIdarray = connection.createArrayOf("Integer", iIdArray);
//            get_related_customer(ci, Warray, iIdarray);
//        }
//    }
//
//    private void get_related_customer(customers_identifier ci, Array Warray, Array iIdarray) {
//        Set<customers_identifier,Integer>
//        selectOrderLineByItemStmt.setArray(1, Warray);
//        selectOrderLineByItemStmt.setArray(2, iIdarray);
//        selectOrderLineByItemStmt.execute();
//        ResultSet resultSet = selectOrderLineByItemStmt.getResultSet();
//        while (resultSet.next()) {
//            int ol_w_id = resultSet.getInt("ol_w_id");
//            int ol_d_id = resultSet.getInt("ol_d_id");
//            int ol_o_id = resultSet.getInt("ol_o_id");
//            int ol_i_id = resultSet.getInt("ol_i_id");
//            customers_identifier cuI = new customers_identifier(ol_w_id,ol_d_id,ol_o_id);
//        }
//
//    }
//}
//
//class customers_identifier {
//    int w_id;
//    int d_id;
//    int o_id;
//
//    public customers_identifier(int w_id, int d_id, int o_id) {
//        this.w_id = w_id;
//        this.d_id = d_id;
//        this.o_id = o_id;
//    }
//
//    public int getW_id() {
//        return w_id;
//    }
//
//    public void setW_id(int w_id) {
//        this.w_id = w_id;
//    }
//
//    public int getD_id() {
//        return d_id;
//    }
//
//    public void setD_id(int d_id) {
//        this.d_id = d_id;
//    }
//
//    public int getO_id() {
//        return o_id;
//    }
//
//    public void setO_id(int o_id) {
//        this.o_id = o_id;
//    }
//}
