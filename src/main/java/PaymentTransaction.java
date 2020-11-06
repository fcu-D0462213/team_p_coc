
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class PaymentTransaction {
    private PreparedStatement selectWarehouseStmt;
    private PreparedStatement selectDistrictStmt;
    private PreparedStatement selectCustomerStmt;
    private PreparedStatement updateWarehouseYTDStmt;
    private PreparedStatement updateDistrictYTDStmt;
    private PreparedStatement updateCustomerByPaymentStmt;
    private ResultSet targetWarehouse;
    private ResultSet targetDistrict;
    private ResultSet targetCustomer;
    public Connection connection;

    private static final String MESSAGE_WAREHOUSE = "Warehouse address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_DISTRICT = "District address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_CUSTOMER = "Customer: Identifier(%1$s, %2$s, %3$s), Name(%4$s, %5$s, %6$s), "
            + "Address(%7$s, %8$s, %9$s, %10$s, %11$s), Phone(%12$s), Since(%13$s), Credits(%14$s, %15$s, %16$s, %17$s)";
    private static final String MESSAGE_PAYMENT = "Payment amount: %1$s";
    private static final String SELECT_WAREHOUSE =
            "SELECT w_ytd, w_street_1, w_street_2, w_city, w_state, w_zip "
                    + "FROM warehouse "
                    + "WHERE w_id = ?;";
    private static final String UPDATE_WAREHOUSE_YTD =
            "UPDATE warehouse "
                    + "SET w_ytd = ? "
                    + "WHERE w_id = ?;";
    private static final String SELECT_DISTRICT =
            "SELECT d_ytd, d_street_1, d_street_2, d_city, d_state, d_zip "
                    + "FROM district "
                    + "WHERE d_w_id = ? AND d_id = ?;";
    private static final String UPDATE_DISTRICT_YTD =
            "UPDATE district "
                    + "SET d_ytd = ? "
                    + "WHERE d_w_id = ? AND d_id = ?;";
    private static final String SELECT_CUSTOMER =
            "SELECT c_balance, c_ytd_payment, c_payment_cnt, c_first, c_middle, c_last, c_street_1, c_street_2, "
                    + "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
                    + "c_discount, c_balance "
                    + "FROM customer "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";


    private static final String UPDATE_CUSTOMER_BY_PAYMENT =
            "UPDATE customer "
                    + "SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";

    PaymentTransaction(Connection connection) {
        this.connection = connection;
        try {
            this.selectWarehouseStmt = connection.prepareStatement(SELECT_WAREHOUSE);
            this.selectDistrictStmt = connection.prepareStatement(SELECT_DISTRICT);
            this.selectCustomerStmt = connection.prepareStatement(SELECT_CUSTOMER);
            this.updateWarehouseYTDStmt = connection.prepareStatement(UPDATE_WAREHOUSE_YTD);
            this.updateDistrictYTDStmt = connection.prepareStatement(UPDATE_DISTRICT_YTD);
            this.updateCustomerByPaymentStmt = connection.prepareStatement(UPDATE_CUSTOMER_BY_PAYMENT);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    void processPayment(int wId, int dId, int cId, float payment) {
        BigDecimal payment_decimal = new BigDecimal(payment);

        selectWarehouse(wId);
        updateWarehouseYTD(wId, payment_decimal);
        selectDistrict(wId, dId);
        updateDistrictYTD(wId, dId, payment_decimal);
        selectCustomer(wId, dId, cId);
        updateCustomerByPayment(wId, dId, cId, payment_decimal);
        outputPaymentResults(wId,dId,cId,payment);
    }


    private void selectWarehouse(final int wId) {
        try {
            selectWarehouseStmt.setInt(1, wId);
            if (selectWarehouseStmt.execute()) {
                ResultSet resultSet = selectWarehouseStmt.getResultSet();
                targetWarehouse = resultSet;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void selectDistrict(final int w_id, final int d_id) {

        try {
            selectDistrictStmt.setInt(1, w_id);
            selectDistrictStmt.setInt(2, d_id);
            if (selectDistrictStmt.execute()) {
                ResultSet resultSet = selectDistrictStmt.getResultSet();
                targetDistrict = resultSet;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void selectCustomer(final int w_id, final int d_id, final int c_id) {


        try {
            selectCustomerStmt.setInt(1, w_id);
            selectCustomerStmt.setInt(2, d_id);
            selectCustomerStmt.setInt(3, c_id);
            if (selectCustomerStmt.execute()) {
                ResultSet resultSet = selectCustomerStmt.getResultSet();
                targetCustomer = resultSet;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void updateWarehouseYTD(final int w_id, final BigDecimal payment) {
        BigDecimal w_ytd = null;
        try {
            targetWarehouse.next();
            w_ytd = targetWarehouse.getBigDecimal("w_ytd").add(payment);
            updateWarehouseYTDStmt.setBigDecimal(1, w_ytd);
            updateWarehouseYTDStmt.setInt(2, w_id);

            updateWarehouseYTDStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void updateDistrictYTD(final int w_id, final int d_id, final BigDecimal payment) {
        BigDecimal d_ytd = null;
        try {
            targetDistrict.next();
            d_ytd = targetDistrict.getBigDecimal("d_ytd").add(payment);
            updateDistrictYTDStmt.setBigDecimal(1, d_ytd);
            updateDistrictYTDStmt.setInt(2, w_id);
            updateDistrictYTDStmt.setInt(3, d_id);

            updateDistrictYTDStmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateCustomerByPayment(final int w_id, final int d_id, final int c_id, final BigDecimal payment) {
        BigDecimal c_balance = null;
        float c_ytd_payment = 0;
        int c_payment_cnt = 0;
        try {
            targetCustomer.next();
            c_balance = targetCustomer.getBigDecimal("c_balance").subtract(payment);
            c_ytd_payment = targetCustomer.getFloat("c_ytd_payment") + payment.floatValue();
            c_payment_cnt = targetCustomer.getInt("c_payment_cnt") + 1;

            updateCustomerByPaymentStmt.setBigDecimal(1, c_balance);
            updateCustomerByPaymentStmt.setFloat(2, c_ytd_payment);
            updateCustomerByPaymentStmt.setInt(3, c_payment_cnt);
            updateCustomerByPaymentStmt.setInt(4, w_id);
            updateCustomerByPaymentStmt.setInt(5, d_id);
            updateCustomerByPaymentStmt.setInt(6, c_id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void outputPaymentResults(int wId, int dId, int cId,float payment) {
        try {
            System.out.println(String.format(MESSAGE_WAREHOUSE,
                    targetWarehouse.getString("w_street_1"),
                    targetWarehouse.getString("w_street_2"),
                    targetWarehouse.getString("w_city"),
                    targetWarehouse.getString("w_state"),
                    targetWarehouse.getString("w_zip")));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            System.out.println(String.format(MESSAGE_DISTRICT,
                    targetDistrict.getString("d_street_1"),
                    targetDistrict.getString("d_street_2"),
                    targetDistrict.getString("d_city"),
                    targetDistrict.getString("d_state"),
                    targetDistrict.getString("d_zip")));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            System.out.println(String.format(MESSAGE_CUSTOMER,
                    wId,dId,cId,

                    targetCustomer.getString("c_first"),
                    targetCustomer.getString("c_middle"),
                    targetCustomer.getString("c_last"),

                    targetCustomer.getString("c_street_1"),
                    targetCustomer.getString("c_street_2"),
                    targetCustomer.getString("c_city"),
                    targetCustomer.getString("c_state"),
                    targetCustomer.getString("c_zip"),

                    targetCustomer.getString("c_phone"),
                    targetCustomer.getTimestamp("c_since"),

                    targetCustomer.getString("c_credit"),
                    targetCustomer.getBigDecimal("c_credit_lim"),
                    targetCustomer.getBigDecimal("c_discount"),
                    targetCustomer.getBigDecimal("c_balance")));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println(String.format(MESSAGE_PAYMENT, payment));
    }


    /*  End of private methods */
}
