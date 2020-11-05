

import com.sun.rowset.internal.Row;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class StockLevelTransaction {

    public Connection connection;

    StockLevelTransaction(Connection connection) {
        this.connection = connection;
    }

    void processStockLevel(int w_ID, int d_ID, int T, int L) {

        //get Last L order number

        String getNextId = "SELECT d_next_o_id FROM " + " district " +
                "WHERE d_w_id = " + w_ID + " AND d_id = " + d_ID;

        try {
            ResultSet resultSet = connection.createStatement().executeQuery(getNextId);
            resultSet.next();
            int d_next_o_id = resultSet.getInt("d_next_o_id");
            int min_id = d_next_o_id - L;
            // ResultSet lastOrderList = session.execute(getLastLOrderNumber);
            Set<Integer> items = new HashSet<>();
            while (resultSet.next()) {
                //int lastOrder = resultSet.getInt("O_ID");

                //get all items
                String getAllItems = "SELECT OL_I_ID FROM " + " order_line " +
                        "WHERE OL_W_ID = " + w_ID + " AND OL_D_ID = " + d_ID + " AND OL_O_ID >= " + min_id + " AND OL_O_ID < " + d_next_o_id;
                ResultSet itemList = connection.createStatement().executeQuery(getAllItems);
                //session.execute(getAllItems);

                while (itemList.next()) {
                    int itemID = itemList.getInt("OL_I_ID");
                    items.add(itemID);
                }
            }


            //HashMap<Integer, Integer> itemQuantity = new HashMap<Integer, Integer>();

            int cnt = 0;

            for (int itemID : items) {
                //check if item quantity in the stock is below the threshold
                String getQuantity = "SELECT S_QUANTITY FROM " + " stock " +
                        "WHERE S_W_ID = " + w_ID + " AND S_I_ID = " + itemID + ";";


                ResultSet resultSet2 = connection.createStatement().executeQuery(getQuantity);
                resultSet2.next();
                //session.execute(getQuantity).one().getDecimal("S_QUANTITY");
                BigDecimal quantity = resultSet2.getBigDecimal("S_QUANTITY");
                BigDecimal decimalT = new BigDecimal(T);

                if (quantity.compareTo(decimalT) == -1) {
                    //itemQuantity.put(itemID, quantity);
                    cnt += 1;
                }
            }
            System.out.println("There are " + cnt + "items below the threshold " + T);
            //System.out.println("Items " + itemID + " stock quantity at " + w_ID + " is below the threshold; its quantity number is: " + quantity + ".");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("\n\n");
    }
}
