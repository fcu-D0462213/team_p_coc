
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class OrderStatusTransaction {

    public Connection connection;

    OrderStatusTransaction(Connection connection) {
        this.connection = connection;
    }

    void processOrderStatus(int c_W_ID, int c_D_ID, int c_ID) {
        String getCustomerName = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM " + "customer " +
                "WHERE C_W_ID = " + c_W_ID + " AND C_D_ID = " + c_D_ID + " AND C_ID = " + c_ID;
        String firstName = null;
        String middleName = null;
        String lastName = null;
        BigDecimal balance = null;
        int last_order = 0;
        Date entry_d = null;
        int carrier_id = 0;
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(getCustomerName);
            resultSet.next();

            firstName = resultSet.getString("C_FIRST");
            middleName = resultSet.getString("C_MIDDLE");
            lastName = resultSet.getString("C_LAST");
            balance = resultSet.getBigDecimal("C_BALANCE");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Customer's first name is: " + firstName + ", middle name is: " + middleName + ", last name is: " + lastName + ".");
        System.out.println("Customer's balance is: " + balance + ".");

        String getOrder = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + "order_sort_by_customer " +
                "WHERE O_W_ID = " + c_W_ID + " AND O_D_ID = " + c_D_ID + " AND O_C_ID = " + c_ID;

        try {
            ResultSet order = connection.createStatement().executeQuery(getOrder);
            order.next();
            last_order = order.getInt("O_ID");
            entry_d = order.getTimestamp("O_ENTRY_D");
            carrier_id = order.getInt("O_CARRIER_ID");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Last order number is: " + last_order + ".");
        System.out.println("Last order entry date and time is: " + entry_d + ".");
        System.out.println("Last order carrier identifier is: " + carrier_id + ".");
        System.out.println("=======Item Info is below.======");

        String getItems = "SELECT OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY FROM " + " order_line " +
                "WHERE OL_W_ID = " + c_W_ID + " AND OL_D_ID = " + c_D_ID + " AND OL_O_ID = " + last_order;

        try {
            ResultSet resultSet2 = connection.createStatement().executeQuery(getItems);
            int itemID;
            int supplierWarehouse;
            BigDecimal quantity;
            BigDecimal amount;
            Date deliveryData;
            while (resultSet2.next()) {

                itemID = resultSet2.getInt("OL_I_ID");
                supplierWarehouse = resultSet2.getInt("OL_SUPPLY_W_ID");
                quantity = resultSet2.getBigDecimal("OL_QUANTITY");
                amount = resultSet2.getBigDecimal("OL_AMOUNT");
                deliveryData = resultSet2.getTimestamp("OL_DELIVERY_D");


                System.out.println("Item number: " + itemID);
                System.out.println("Supplying warehouse number: " + supplierWarehouse);
                System.out.println("Quantity ordered: " + quantity);
                System.out.println("Total price for ordered item: " + amount);
                System.out.println("Data and time of delivery: " + deliveryData);
                System.out.println("\n\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
