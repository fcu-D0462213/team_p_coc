
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class TopBalanceTransaction {

    private PreparedStatement selectCustomerNameStmt;

    public Connection connection;

    private static final String SELECT_CUSTOMER_NAME =
            "SELECT c_w_id, c_balance, c_d_id, c_first, c_middle, c_last, c_w_name, c_d_name "
                    + "FROM customer_sort_by_balance "
                    + "WHERE c_w_id= ? LIMIT ?;";

    TopBalanceTransaction(Connection connection) {
        this.connection = connection;
        try {
            selectCustomerNameStmt = connection.prepareStatement(SELECT_CUSTOMER_NAME);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /* Start of public methods */
    void topBalance() {
        for (int i = 1; i < 11; i++) {
            try {
                selectCustomerNameStmt.setInt(1, i);
                selectCustomerNameStmt.setInt(2, 10);
                selectCustomerNameStmt.execute();
                ResultSet customer = selectCustomerNameStmt.getResultSet();
                customer.next();
                System.out.println("customer name: " + customer.getString("c_first") + " "
                        + customer.getString("c_middle") + " " + customer.getString("c_last"));
                System.out.println("customer balance: " + customer.getBigDecimal("c_balance").floatValue());
                System.out.println("Warehouse name: " + customer.getString("c_w_name") + ",District name: " + customer.getString("c_d_name"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }



}
