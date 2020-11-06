import java.sql.*;
import java.util.*;

public class RelatedCustomerTransaction {
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement selectOrderLineStmt;
    private PreparedStatement selectOrderWithItemCount;
    private PreparedStatement selectCustomerByOrderStmt;
    public Connection connection;

    private static final String SELECT_CUSTOMER =
            "SELECT o_id "
                    + "FROM order_sort_by_customer "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?;";
    private static final String SELECT_ORDER_LINE =
            "SELECT ol_i_id "
                    + "FROM order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_ORDER_WITH_ITEM_COUNT =
            "SELECT ol_w_id, ol_d_id, ol_o_id, sum(1) as count "
                    + "FROM order_line WHERE ol_w_id != ? and ol_i_id in ? "
                    + "GROUP BY ol_w_id, ol_d_id, ol_o_id;";
    private static final String SELECT_CUSTOMER_BY_ORDER =
            "SELECT o_c_id "
                    + "FROM orders "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;";

    RelatedCustomerTransaction(Connection connection) {
        this.connection = connection;
        try {
            this.selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
            this.selectOrderLineStmt = connection.prepareStatement(SELECT_ORDER_LINE);
            this.selectOrderWithItemCount = connection.prepareStatement(SELECT_ORDER_WITH_ITEM_COUNT);
            this.selectCustomerByOrderStmt = connection.prepareStatement(SELECT_CUSTOMER_BY_ORDER);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    void processRelatedCustomer(int w_ID, int d_ID, int c_ID) {
        //Object[] wIdArray;
        //Object[] iIdArray;
        //Set<String> relatedCustomers = new HashSet<>();

        try {
            selectCustomerStmt.setInt(1, w_ID);
            selectCustomerStmt.setInt(2, d_ID);
            selectCustomerStmt.setInt(3, c_ID);
            selectCustomerStmt.execute();
            ResultSet orders = selectCustomerStmt.getResultSet();
            while (orders.next()) {
                List<Integer> iArray = new ArrayList<>();
                int o_ID = orders.getInt("o_id");
                selectOrderLineStmt.setInt(1, w_ID);
                selectOrderLineStmt.setInt(2, d_ID);
                selectOrderLineStmt.setInt(3, o_ID);
                selectOrderLineStmt.execute();
                ResultSet i_ids = selectOrderLineStmt.getResultSet();
                StringBuffer sb = new StringBuffer();
                sb.append("(");
                while (i_ids.next()) {
                    int i_id = i_ids.getInt("ol_i_id");
                    sb.append(i_id);
                    sb.append(",");
                    //iArray.add(i_id);
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                String iIdarray = sb.toString();
                //iIdArray = iArray.toArray();
                //Array iIdarray = connection.createArrayOf("Integer", iIdArray);
                System.out.println(iIdarray);
                System.out.println("Customer identifier: " + w_ID + " " + d_ID + " " + c_ID);
                get_related_customer(w_ID, iIdarray);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //return relatedCustomers;
    }

    private void get_related_customer(int w_ID, String iIdarray) {
        String SELECT_ORDER_WITH_ITEM_COUNT =
                "SELECT ol_w_id, ol_d_id, ol_o_id, sum(1) as count "
                        + "FROM order_line WHERE ol_w_id != " + w_ID + " and ol_i_id in " + iIdarray
                        + " GROUP BY ol_w_id, ol_d_id, ol_o_id;";
        //Set<customers_identifier,Integer>
        try {
            //selectOrderWithItemCount.setInt(1, w_ID);
            //selectOrderWithItemCount.setString(2, iIdarray);
            ResultSet resultSet = connection.createStatement().executeQuery(SELECT_ORDER_WITH_ITEM_COUNT);
            //selectOrderWithItemCount.execute();
            //ResultSet resultSet = selectOrderWithItemCount.getResultSet();
            while (resultSet.next()) {
                int ol_w_id = resultSet.getInt("ol_w_id");
                int ol_d_id = resultSet.getInt("ol_d_id");
                int ol_o_id = resultSet.getInt("ol_o_id");
                int count = resultSet.getInt("count");
                if (count >= 2) {
                    selectCustomerByOrderStmt.setInt(1, ol_w_id);
                    selectCustomerByOrderStmt.setInt(2, ol_d_id);
                    selectCustomerByOrderStmt.setInt(3, ol_o_id);
                    selectCustomerByOrderStmt.execute();
                }

                ResultSet resultSet2 = selectCustomerByOrderStmt.getResultSet();
                if (resultSet2 == null) {
                    //System.out.println("There is no identifier");
                } else {
                    while (resultSet2.next()){
                        int o_c_id = resultSet2.getInt("o_c_id");
                        System.out.println("Identifier info: ol_w_id: " + ol_w_id + " &ol_d_id: " + ol_d_id + " &o_c_id: " + o_c_id);
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}