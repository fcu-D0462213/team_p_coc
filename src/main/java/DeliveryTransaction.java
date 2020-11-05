

import com.sun.rowset.internal.Row;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.List;

public class DeliveryTransaction {
    private PreparedStatement selectSmallestOrderStmt;
    private PreparedStatement selectOrderLinesStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateOrderByIdStmt;
    private PreparedStatement updateOrderByTimestampStmt;
    private PreparedStatement updateOrderLineStmt;
    private PreparedStatement updateCustomerByDeliveryStmt;
    private ResultSet targetCustomer;
    private ResultSet targetOrder;
    private Connection connection;

    private static final String SELECT_SMALLEST_ORDER =
            "SELECT o_id, o_c_id, o_entry_d "
                    + "FROM orders_by_id "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = -1 LIMIT 1;";
    private static final String SELECT_ORDER_LINES =
            "SELECT ol_number, ol_amount "
                    + "FROM order_lines "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_balance, c_delivery_cnt "
                    + "FROM customers "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_CUSTOMER_BY_DELIVERY =
            "UPDATE customers "
                    + "SET c_balance = ?, c_delivery_cnt = ?, c_carrier_id = ?"
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String UPDATE_ORDER_LINE =
            "UPDATE order_lines "
                    + "SET ol_delivery_d = ? "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?;";
    private static final String UPDATE_ORDER_BY_ID =
            "UPDATE orders_by_id "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND o_c_id = ?;";
    private static final String UPDATE_ORDER_BY_TIMESTAMP =
            "UPDATE orders_by_timestamp "
                    + "SET o_carrier_id = ? "
                    + "WHERE o_w_id = ? AND o_d_id = ? AND o_entry_d = ? AND o_id = ? AND o_c_id = ?;";

    DeliveryTransaction(Connection connection) {
        this.connection = connection;
        try {
            this.selectSmallestOrderStmt = connection.prepareStatement(SELECT_SMALLEST_ORDER);
            this.selectOrderLinesStmt = connection.prepareStatement(SELECT_ORDER_LINES);
            this.selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
            this.updateOrderByIdStmt = connection.prepareStatement(UPDATE_ORDER_BY_ID);
            this.updateOrderByTimestampStmt = connection.prepareStatement(UPDATE_ORDER_BY_TIMESTAMP);
            this.updateOrderLineStmt = connection.prepareStatement(UPDATE_ORDER_LINE);
            this.updateCustomerByDeliveryStmt = connection.prepareStatement(UPDATE_CUSTOMER_BY_DELIVERY);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /* Start of public methods */

    /**
     *
     * @param wId : used for customer identifier
     * @param carrierId : used for carrier identifier
     */
    void processDelivery(int wId, int carrierId) {
        for (int dId=1; dId<=10; dId++) {
            selectSmallestOrder(wId, dId);
            int oId = 0;
            int cId = 0;
            Date o_entry_d = null;
            try {
                targetOrder.next();
                oId = targetOrder.getInt("o_id");
                cId = targetOrder.getInt("o_c_id");
                o_entry_d = targetOrder.getTimestamp("o_entry_d");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            updateOrderByCarrier(wId, dId, cId, oId, carrierId, o_entry_d);

            Date ol_delivery_d = new Date();
            BigDecimal olAmountSum = selectAndUpdateOrderLines(wId, dId, oId, ol_delivery_d);
            selectCustomer(wId, dId, cId);
            updateCustomerByDelivery(wId, dId, cId, olAmountSum, carrierId);
        }
    }

    /*  End of public methods */

    /*  Start of private methods */

    private void selectSmallestOrder(final int w_id, final int d_id) {
        try {
            selectSmallestOrderStmt.setInt(1,w_id);
            selectSmallestOrderStmt.setInt(2,d_id);
            if (selectSmallestOrderStmt.execute()) {
                ResultSet resultSet = selectSmallestOrderStmt.getResultSet();
                if (resultSet.next()) {
                    targetOrder = resultSet;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //ResultSet resultSet = session.execute(selectSmallestOrderStmt.bind(w_id, d_id));
        //List<Row> orders = resultSet.all();
        //
        //if(!orders.isEmpty()) {
        //    targetOrder = orders.get(0);
        //}
    }

    private BigDecimal selectAndUpdateOrderLines(final int w_id, final int d_id, final int o_id, final Date ol_delivery_d) {
        BigDecimal sum = new BigDecimal(0);
        try {
            selectOrderLinesStmt.setInt(1, w_id);
            selectOrderLinesStmt.setInt(2, d_id);
            selectOrderLinesStmt.setInt(3, o_id);
            if (selectOrderLinesStmt.execute()) {
                ResultSet resultSet = selectOrderLinesStmt.getResultSet();
                while (resultSet.next()) {
                    BigDecimal partial_sum = resultSet.getBigDecimal("ol_amount");
                    int ol_number = resultSet.getInt("ol_number");
                    sum = sum.add(partial_sum);
                    updateOrderLineStmt.setTimestamp(1, new Timestamp(ol_delivery_d.getTime()));
                    updateOrderLineStmt.setInt(2, w_id);
                    updateOrderLineStmt.setInt(3, d_id);
                    updateOrderLineStmt.setInt(4, o_id);
                    updateOrderLineStmt.setInt(5, ol_number);
                    updateOrderLineStmt.execute();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //ResultSet resultSet = session.execute(selectOrderLinesStmt.bind(w_id, d_id, o_id));
        //List<Row> resultRow = resultSet.all();
        //
        //
        //for (int i=0; i<resultRow.size(); i++) {
        //    Row orderLine = resultRow.get(i);
        //    BigDecimal partial_sum = orderLine.getDecimal("ol_amount");
        //    int ol_number = orderLine.getInt("ol_number");
        //
        //    sum = sum.add(partial_sum);
        //
        //    session.execute(updateOrderLineStmt.bind(ol_delivery_d, w_id, d_id, o_id, ol_number));
        //}

        return sum;
    }

    private void selectCustomer(final int w_id, final int d_id, final int c_id) {
        try {
            selectCustomerStmt.setInt(1, w_id);
            selectCustomerStmt.setInt(2, d_id);
            selectCustomerStmt.setInt(3, c_id);

            if (selectCustomerStmt.execute()) {
                ResultSet resultSet = selectCustomerStmt.getResultSet();
                if (resultSet.next()) {
                    targetCustomer = resultSet;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //ResultSet resultSet = session.execute(selectCustomerStmt.bind(w_id, d_id, c_id));
        //List<Row> customers = resultSet.all();
        //
        //if(!customers.isEmpty()) {
        //    targetCustomer = customers.get(0);
        //}
    }

    private void updateOrderByCarrier(final int w_id, final int d_id, final int c_id, final int o_id, final int carrier_id, final Date o_entry_d) {
        try {
            updateOrderByIdStmt.setInt(1,carrier_id);
            updateOrderByIdStmt.setInt(2,w_id);
            updateOrderByIdStmt.setInt(3,d_id);
            updateOrderByIdStmt.setInt(4,o_id);
            updateOrderByIdStmt.setInt(5,c_id);

            updateOrderByTimestampStmt.setInt(1,carrier_id);
            updateOrderByTimestampStmt.setInt(2,w_id);
            updateOrderByTimestampStmt.setInt(3,d_id);
            updateOrderByTimestampStmt.setTimestamp(4, new Timestamp(o_entry_d.getTime()));
            updateOrderByTimestampStmt.setInt(5, o_id);
            updateOrderByTimestampStmt.setInt(6, c_id);

            updateOrderByIdStmt.execute();
            updateOrderByTimestampStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //session.execute(updateOrderByIdStmt.bind(carrier_id, w_id, d_id, o_id, c_id));
        //session.execute(updateOrderByTimestampStmt.bind(carrier_id, w_id, d_id, o_entry_d, o_id, c_id));
    }

    private void updateCustomerByDelivery(final int w_id, final int d_id, final int c_id, final BigDecimal ol_amount_sum, final int carrier_id) {
        try {
            targetCustomer.next();
            BigDecimal c_balance = targetCustomer.getBigDecimal("c_balance").add(ol_amount_sum);
            int c_delivery_cnt = targetCustomer.getInt("c_delivery_cnt") + 1;
            updateCustomerByDeliveryStmt.setBigDecimal(1, c_balance);
            updateCustomerByDeliveryStmt.setInt(2, c_delivery_cnt);
            updateCustomerByDeliveryStmt.setInt(3, carrier_id);
            updateCustomerByDeliveryStmt.setInt(4, w_id);
            updateCustomerByDeliveryStmt.setInt(5, d_id);
            updateCustomerByDeliveryStmt.setInt(6, c_id);
            updateCustomerByDeliveryStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //session.execute(updateCustomerByDeliveryStmt.bind(c_balance, c_delivery_cnt, carrier_id, w_id, d_id, c_id));
    }

    /*  End of private methods */
}
