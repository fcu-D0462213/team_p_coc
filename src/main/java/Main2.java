

import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;





public class Main2 {

    public static void main(String[] args) {
        String tranDir = args[0];
        String cliId = args[1];
        String repFile = args[2];
        String ip = args[3];
        List<Long> timeList = new ArrayList<>();
        Transactions transaction = new Transactions(ip);
        BufferedReader bf = null;
        try {
            FileReader fr = new FileReader(tranDir + "/" + cliId + ".txt");
            bf = new BufferedReader(fr);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Scanner sc = new Scanner(bf);
        String[] lineData;
        long totalStartTime = System.currentTimeMillis();
        while (sc.hasNext()) {
            //transactionCount++;
            lineData = sc.nextLine().split(",");
            long startTime = System.currentTimeMillis();
            switch (lineData[0]) {
                case "N":
                    int itemCount = Integer.parseInt(lineData[lineData.length - 1]);

                    List<List<Integer>> itemOrders = new ArrayList<>();
                    for (int i = 0; i < itemCount; i++) {
                        String[] orderString = sc.nextLine().split(",");
                        List<Integer> order = new ArrayList<>();
                        for (String s : orderString) {
                            order.add(Integer.parseInt(s));
                        }
                        itemOrders.add(order);
                    }
                    transaction.processOrder(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]), Integer.parseInt(lineData[3]), itemOrders);
                    break;
                case "P":
                    transaction.processPayment(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]), Integer.parseInt(lineData[3]), Float.parseFloat(lineData[4]));
                    break;
                case "D":
                    transaction.processDelivery(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]));
                    break;
                case "O":
                    transaction.processOrderStatus(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]), Integer.parseInt(lineData[3]));
                    break;
                case "S":
                    transaction.processStockLevel(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]), Integer.parseInt(lineData[3]), Integer.parseInt(lineData[4]));
                    break;
                case "I":
                    transaction.popularItem(Integer.parseInt(lineData[1]),
                            Integer.parseInt(lineData[2]),
                            Integer.parseInt(lineData[3]));
                    break;
                case "T":
                    transaction.topBalance();
                    break;
                case "R":
                    transaction.processRelatedCustomer(Integer.parseInt(lineData[1]),
                            Integer.parseInt(lineData[2]),
                            Integer.parseInt(lineData[3]));
                    break;
                default:
                    System.out.println("Unsupported transaction type!");
            }
            long endTime = System.currentTimeMillis();
            long time =  endTime -startTime;
            timeList.add(time);
        }
        long totalEndTime = System.currentTimeMillis();
        Collections.sort(timeList);

        double[] timeArray = new double[timeList.size()];
        for (int i = 0; i <timeList.size() ; i++) {
            timeArray[i] = timeList.get(i);
        }
        long totalTran = timeList.size();
        long totalTimeMs = totalEndTime-totalStartTime;
        Median median = new Median();
        double mediaTran = median.evaluate(timeArray);
        double tran95 = StatUtils.percentile(timeArray,95.0);
        double tran99 = StatUtils.percentile(timeArray,99.0);
        double totalTimeSe = (double) totalTimeMs / 1000;
        double throughput = (double) totalTran / totalTimeSe;
        double avaLatency = (double) totalTimeMs / totalTran;

        PrintWriter out = null;
        try {
            out = new PrintWriter(repFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        out.print(totalTran + "," + totalTimeSe + "," + throughput + "," +
                mediaTran + "," + avaLatency + "," + tran95 + "," + tran99);
        out.close();

    }

}