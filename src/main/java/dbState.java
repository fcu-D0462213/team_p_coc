import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class dbState {
    private static Connection connection;
    //public Connection connection;

    public static void main(String[] args) {
        String fileName = args[0];
        String ip = args[1];
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

        String sql1 = "SELECT sum(w_ytd) FROM warehouse";
        String sql2 = "SELECT sum(d_ytd), sum(d_next_o_id) FROM district";
        String sql3 = "SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) FROM customer";
        String sql4 = "SELECT max(o_id), sum(o_ol_cnt) FROM orders";
        String sql5 = "SELECT sum(ol_amount), sum(ol_quantity) FROM order_line";
        String sql6 = "SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) FROM stock";

        String sum_w_ytd = null;
        String sum_d_ytd = null;
        String sum_d_next_o_id = null;
        String sum_c_balance = null;
        String sum_c_ytd_payment = null;
        String sum_c_payment_cnt = null;
        String sum_c_delivery_cnt = null;
        String max_o_id = null;
        String sum_o_ol_cnt = null;
        String sum_ol_amount = null;
        String sum_ol_quantity = null;
        String sum_s_quantity = null;
        String sum_s_ytd = null;
        String sum_s_order_cnt = null;
        String sum_s_remote_cnt = null;
        try {
            System.out.println("Start dbState");
            ResultSet resultSet1 = connection.createStatement().executeQuery(sql1);
            resultSet1.next();
            sum_w_ytd = resultSet1.getString(1);

            ResultSet resultSet2 = connection.createStatement().executeQuery(sql2);
            resultSet2.next();
            sum_d_ytd = resultSet2.getString(1);
            sum_d_next_o_id = resultSet2.getString(2);

            ResultSet resultSet3 = connection.createStatement().executeQuery(sql3);
            resultSet3.next();
            sum_c_balance = resultSet3.getString(1);
            sum_c_ytd_payment = resultSet3.getString(2);
            sum_c_payment_cnt = resultSet3.getString(3);
            sum_c_delivery_cnt = resultSet3.getString(4);

            ResultSet resultSet4 = connection.createStatement().executeQuery(sql4);
            resultSet4.next();
            max_o_id = resultSet4.getString(1);
            sum_o_ol_cnt = resultSet4.getString(2);

            ResultSet resultSet5 = connection.createStatement().executeQuery(sql5);
            resultSet5.next();
            sum_ol_amount = resultSet5.getString(1);
            sum_ol_quantity = resultSet5.getString(2);

            ResultSet resultSet6 = connection.createStatement().executeQuery(sql6);
            resultSet6.next();
            sum_s_quantity = resultSet6.getString(1);
            sum_s_ytd = resultSet6.getString(2);
            sum_s_order_cnt = resultSet6.getString(3);
            sum_s_remote_cnt = resultSet6.getString(4);
            System.out.println("Start dbState");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        StringBuffer sb = new StringBuffer();
        sb.append(sum_w_ytd + ",");
        sb.append(sum_d_ytd + ",");
        sb.append(sum_d_next_o_id + ",");
        sb.append(sum_c_balance + ",");
        sb.append(sum_c_ytd_payment + ",");
        sb.append(sum_c_payment_cnt + ",");
        sb.append(sum_c_delivery_cnt + ",");
        sb.append(max_o_id + ",");
        sb.append(sum_o_ol_cnt + ",");
        sb.append(sum_ol_amount + ",");
        sb.append(sum_ol_quantity + ",");
        sb.append(sum_s_quantity + ",");
        sb.append(sum_s_ytd + ",");
        sb.append(sum_s_order_cnt + ",");
        sb.append(sum_s_remote_cnt);
        String res = sb.toString();
        PrintWriter out = null;
        try {
            out = new PrintWriter(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        out.print(res);
        out.close();
    }
}
