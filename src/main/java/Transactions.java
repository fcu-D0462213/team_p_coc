

import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

class Transactions {


    private Connection connection;
    private OrderTransaction orderTransaction;
    private PaymentTransaction paymentTransaction;
    private DeliveryTransaction deliveryTransaction;
    private OrderStatusTransaction orderStatusTransaction;
    private StockLevelTransaction stockLevelTransaction;
    private PopularItemTransaction popularItemTransaction;
    private TopBalanceTransaction topBalanceTransaction;
    private RelatedCustomerTransaction relatedCustomerTransaction;

    Transactions(String ip) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName(ip);
        ds.setPortNumber(26257);
        ds.setDatabaseName("project");
        ds.setUser("test");
        ds.setPassword(null);
        try {
            connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        orderTransaction = new OrderTransaction(connection);
        paymentTransaction = new PaymentTransaction(connection);
        deliveryTransaction = new DeliveryTransaction(connection);
        orderStatusTransaction = new OrderStatusTransaction(connection);
        stockLevelTransaction = new StockLevelTransaction(connection);
        popularItemTransaction = new PopularItemTransaction(connection);
        topBalanceTransaction = new TopBalanceTransaction(connection);
        relatedCustomerTransaction = new RelatedCustomerTransaction(connection);
    }

    /* Start of public methods */

    void processOrder(int cId, int wId, int dID, List<List<Integer>> itemOrders) {
        orderTransaction.processOrder(cId, wId, dID, itemOrders);
    }

    void processPayment(int wId, int dId, int cId, float payment) {
        paymentTransaction.processPayment(wId, dId, cId, payment);
    }

    void processDelivery(int wId, int carrierId) {
        deliveryTransaction.processDelivery(wId, carrierId);
    }

    void processOrderStatus(int c_W_ID, int c_D_ID, int c_ID) {
        orderStatusTransaction.processOrderStatus(c_W_ID, c_D_ID, c_ID);
    }

    void processStockLevel(int w_ID, int d_ID, int T, int L) {
        stockLevelTransaction.processStockLevel(w_ID, d_ID, T, L);
    }

    void popularItem(int wId, int dId, int numOfOrders) {
        popularItemTransaction.popularItem(wId, dId, numOfOrders);
    }

    void topBalance() {
        topBalanceTransaction.topBalance();
    }

    void processRelatedCustomer(int w_ID, int d_ID, int c_ID) {
        relatedCustomerTransaction.processRelatedCustomer(w_ID,d_ID,c_ID);
    }
}
