

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
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
        String line;
        String[] lineData;
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
        Collections.sort(timeList);

        long totalTran = timeList.size();
        long totalTimeMs = 0;
        long mediaTran = 0;
        long tran95 = 0;
        long tran99 = 0;
        for (int i = 0; i < totalTran; i++) {
            totalTimeMs += timeList.get(i);
            if (i == totalTran / 2) {
                mediaTran = timeList.get(i);
            }
            if (i == 94) {
                tran95 = timeList.get(i);
            }
            if (i == 98) {
                tran99 = timeList.get(i);
            }
        }
        double totalTimeSe = (double) totalTimeMs / 1000;
        double throughput = totalTran / totalTimeSe;
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