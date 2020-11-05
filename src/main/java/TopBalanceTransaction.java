

import com.sun.rowset.internal.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class TopBalanceTransaction {
    /* popular items */
    private PreparedStatement selectTopBalanceStmt;
    private PreparedStatement selectCustomerNameStmt;

    public Connection connection;

    private static final String SELECT_TOP_BALANCE =
            "SELECT * "
                    + "FROM customers_balances "
                    + "LIMIT 10;";
    private static final String SELECT_CUSTOMER_NAME =
            "SELECT c_first, c_middle, c_last "
                    + "FROM customers "
                    + "WHERE c_w_id=? AND c_d_id=? AND c_id = ?;";
    TopBalanceTransaction(Connection connection) {
        this.connection = connection;
        try {
            selectTopBalanceStmt = connection.prepareStatement(SELECT_TOP_BALANCE);
            selectCustomerNameStmt = connection.prepareStatement(SELECT_CUSTOMER_NAME);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /* Start of public methods */
    void topBalance() {
        List<ResultSet> customerNames = new ArrayList();
        ResultSet topCustomers = null;
        try {
            selectTopBalanceStmt.execute();
            topCustomers = selectTopBalanceStmt.getResultSet();
            while (topCustomers.next()) {
                selectCustomerNameStmt.setInt(1,topCustomers.getInt("c_w_id"));
                selectCustomerNameStmt.setInt(2, topCustomers.getInt("c_d_id"));
                selectCustomerNameStmt.setInt(3, topCustomers.getInt("c_id"));
                selectCustomerNameStmt.execute();
                ResultSet customerName = selectCustomerNameStmt.getResultSet();
                customerNames.add(customerName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //List<Row> topCustomers = resultSet.all();
        //List<Row> customerNames = new ArrayList();
        //for(Row cus: topCustomers){
        //    ResultSet customerName = session.execute(selectCustomerNameStmt.bind(cus.getInt("c_w_id"),
        //            cus.getInt("c_d_id"), cus.getInt("c_id")));
        //    Row cusN = (customerName.all()).get(0);
        //    customerNames.add(cusN);
        //}
        outputTopBalance(topCustomers, customerNames);
    }

    /*  End of public methods */

    private void outputTopBalance(ResultSet topCustomers, List<ResultSet> customerNames){
        try {
            while (topCustomers.next()) {
                if (topCustomers.getRow() == 11) {
                    break;
                }
                ResultSet cusName = customerNames.get(topCustomers.getRow());
                cusName.next();
                System.out.println("customer name: " + cusName.getString("c_first") + " "
                        + cusName.getString("c_middle") + " " + cusName.getString("c_last"));
                //Row cus = topCustomers.get(i);
                System.out.println("customer balance: " + (topCustomers.getBigDecimal("c_balance")).intValue());
                System.out.println("WId: " + topCustomers.getInt("c_w_id") + " DId: " + topCustomers.getInt("c_d_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //for (int i=0; i<10; i++){
        //    ResultSet cusName = customerNames.get(i);
        //    System.out.println("customer name: " + cusName.getString("c_first") + " "
        //            + cusName.getString("c_middle") + " " + cusName.getString("c_last"));
        //    Row cus = topCustomers.get(i);
        //    System.out.println("customer balance: " + (cus.getDecimal("c_balance")).intValue());
        //    System.out.println("WId: " + cus.getInt("c_w_id") + " DId: " + cus.getInt("c_d_id"));
        //}
    }

    /*  End of private methods */
}
