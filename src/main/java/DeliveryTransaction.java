
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;

public class DeliveryTransaction {
    private PreparedStatement selectSmallestOrderStmt;
    private PreparedStatement updateSmallestOrderStmt;
    private PreparedStatement updateCarrierStmt;
    private PreparedStatement selectItemInfoStmt;
    private PreparedStatement updateDeliverDStmt;
    private PreparedStatement selectCustomerIDStmt;
    private PreparedStatement selectCustomerInfoStmt;
    private PreparedStatement updateCustomerStmt;

    private ResultSet targetCustomer;
    private ResultSet targetOrder;
    private Connection connection;

    private static final String SELECT_SMALLEST_ORDER =
            "SELECT d_next_o_id, d_next_deliver_o_id FROM district WHERE d_w_id = ? AND d_id = ?";
    private static final String UPDATE_SMALLEST_ORDER =
            "UPDATE district SET d_next_deliver_o_id = ? WHERE d_w_id = ? AND d_id = ?";
    private static final String UPDATE_CARRIER =
            "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?";
    private static final String GET_ITEM_INFO =
            "SELECT ol_number, ol_amount, ol_quantity, ol_i_id FROM order_line"
                    + " WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";
    private static final String UPDATE_DELIVER_D =
            "UPDATE order_line SET ol_delivery_d = ? "
                    + " WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ? AND ol_number = ? AND ol_i_id = ?";
    private static final String SELECT_CUSTOMER_ID =
            "SELECT o_c_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?";
    private static final String SELECT_CUSTOMER_INFO =
            "SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    private static final String UPDATE_CUSTOMER =
            "UPDATE customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";

    DeliveryTransaction(Connection connection) {
        this.connection = connection;
        try {
            this.selectSmallestOrderStmt = connection.prepareStatement(SELECT_SMALLEST_ORDER);
            this.updateSmallestOrderStmt = connection.prepareStatement(UPDATE_SMALLEST_ORDER);
            this.updateCarrierStmt = connection.prepareStatement(UPDATE_CARRIER);
            this.selectItemInfoStmt = connection.prepareStatement(GET_ITEM_INFO);
            this.updateDeliverDStmt = connection.prepareStatement(UPDATE_DELIVER_D);
            this.selectCustomerIDStmt = connection.prepareStatement(SELECT_CUSTOMER_ID);
            this.selectCustomerInfoStmt = connection.prepareStatement(SELECT_CUSTOMER_INFO);
            this.updateCustomerStmt = connection.prepareStatement(UPDATE_CUSTOMER);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    void processDelivery(int wId, int carrierId) {
        for (int dId = 1; dId <= 10; dId++) {
            Integer next_id = processSmallestOrder(wId, dId);
            if (next_id != null) {
                System.out.println("Start delivery");
                updateCarrier(wId, dId, next_id, carrierId);
                BigDecimal olAmountSum = updateDatetime(wId, dId, next_id);
                updateCustomer(wId, dId, next_id, olAmountSum);
                System.out.println("Finish delivery");
            } else {
                System.out.println("delivery fail");

            }

        }
    }


    private Integer processSmallestOrder(final int w_id, final int d_id) {
        int next_order_id;
        int next_deliver_id = 0;
        try {
            selectSmallestOrderStmt.setInt(1, w_id);
            selectSmallestOrderStmt.setInt(2, d_id);
            if (selectSmallestOrderStmt.execute()) {
                ResultSet resultSet = selectSmallestOrderStmt.getResultSet();
                targetOrder = resultSet;
            }
            targetOrder.next();
            next_order_id = targetOrder.getInt("d_next_o_id");
            next_deliver_id = targetOrder.getInt("d_next_deliver_o_id");
            if (next_order_id <= next_deliver_id) {
                return null;
            }
            updateSmallestOrderStmt.setInt(1, next_deliver_id + 1);
            updateSmallestOrderStmt.setInt(2, w_id);
            updateSmallestOrderStmt.setInt(3, d_id);
            updateSmallestOrderStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return next_deliver_id;
    }

    private void updateCarrier(final int w_id, final int d_id, final int o_id, final int carrier_id) {
        try {
            updateCarrierStmt.setInt(1, carrier_id);
            updateCarrierStmt.setInt(2, w_id);
            updateCarrierStmt.setInt(3, d_id);
            updateCarrierStmt.setInt(4, o_id);
            updateCarrierStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private BigDecimal updateDatetime(final int w_id, final int d_id, final int o_id) {
        BigDecimal sum = new BigDecimal(0);
        Date ol_delivery_d = new Date();
        try {
            selectItemInfoStmt.setInt(1, w_id);
            selectItemInfoStmt.setInt(2, d_id);
            selectItemInfoStmt.setInt(3, o_id);
            if (selectItemInfoStmt.execute()) {
                ResultSet resultSet = selectItemInfoStmt.getResultSet();
                while (resultSet.next()) {
                    BigDecimal partial_sum = resultSet.getBigDecimal("ol_amount");
                    int ol_number = resultSet.getInt("ol_number");
                    int ol_i_id = resultSet.getInt("ol_i_id");
                    sum = sum.add(partial_sum);
                    updateDeliverDStmt.setTimestamp(1, new Timestamp(ol_delivery_d.getTime()));
                    updateDeliverDStmt.setInt(2, w_id);
                    updateDeliverDStmt.setInt(3, d_id);
                    updateDeliverDStmt.setInt(4, o_id);
                    updateDeliverDStmt.setBigDecimal(5, resultSet.getBigDecimal("ol_quantity"));
                    updateDeliverDStmt.setInt(6, ol_number);
                    updateDeliverDStmt.setInt(7, ol_i_id);
                    updateDeliverDStmt.execute();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sum;
    }

    private void updateCustomer(final int w_id, final int d_id, final int o_id, final BigDecimal ol_amount_sum) {
        try {
            selectCustomerIDStmt.setInt(1, w_id);
            selectCustomerIDStmt.setInt(2, d_id);
            selectCustomerIDStmt.setInt(3, o_id);

            if (selectCustomerIDStmt.execute()) {
                ResultSet resultSet = selectCustomerIDStmt.getResultSet();
                targetCustomer = resultSet;
            }
            targetCustomer.next();
            int cId = targetCustomer.getInt("o_c_id");

            selectCustomerInfoStmt.setInt(1, w_id);
            selectCustomerInfoStmt.setInt(2, d_id);
            selectCustomerInfoStmt.setInt(3, cId);
            selectCustomerInfoStmt.execute();

            ResultSet resultSet2 = selectCustomerInfoStmt.getResultSet();
            resultSet2.next();

            BigDecimal c_balance = resultSet2.getBigDecimal("c_balance").add(ol_amount_sum);
            int c_delivery_cnt = resultSet2.getInt("c_delivery_cnt") + 1;
            updateCustomerStmt.setBigDecimal(1, c_balance);
            updateCustomerStmt.setInt(2, c_delivery_cnt);
            updateCustomerStmt.setInt(3, w_id);
            updateCustomerStmt.setInt(4, d_id);
            updateCustomerStmt.setInt(5, cId);
            updateCustomerStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
