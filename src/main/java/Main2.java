import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;


//class ClientThread implements Callable<Triple<Integer, Long, Double>> {
//    private int index;
//    private Transactions transaction;
//
//    ClientThread(int index) {
//        this.index = index;
//    }
//
//    private long readTransaction(Scanner sc) {
//        int transactionCount = 0;
//        while (sc.hasNext()) {
//            transactionCount++;
//            String[] instruction = sc.nextLine().split(",");
//            switch (instruction[0]) {
//                case "N":
//                    // new order transaction
//                    int itemCount = Integer.parseInt(instruction[instruction.length - 1]);
//
//                    List<List<Integer>> itemOrders = new ArrayList<>();
//                    for (int i = 0; i < itemCount; i++) {
//                        String[] orderString = sc.nextLine().split(",");
//                        List<Integer> order = new ArrayList<>();
//                        for (String s : orderString) {
//                            order.add(Integer.parseInt(s));
//                        }
//                        itemOrders.add(order);
//                    }
//
//                    transaction.processOrder(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), itemOrders);
//                    break;
//                case "P":
//                    // payment transaction
//                    transaction.processPayment(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), Float.parseFloat(instruction[4]));
//                    break;
//                case "D":
//                    // delivery transaction
//                    transaction.processDelivery(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]));
//                    break;
//                case "O":
//                    transaction.processOrderStatus(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]));
//                    break;
//                case "S":
//                    transaction.processStockLevel(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), Integer.parseInt(instruction[4]));
//                    break;
//                case "I":
//                    transaction.popularItem(Integer.parseInt(instruction[1]),
//                            Integer.parseInt(instruction[2]),
//                            Integer.parseInt(instruction[3]));
//                    break;
//                case "T":
//                    transaction.topBalance();
//                    break;
//                case "R":
//                    transaction.processRelatedCustomer(Integer.parseInt(instruction[1]),
//                            Integer.parseInt(instruction[2]),
//                            Integer.parseInt(instruction[3]));
//                default:
//                    System.out.println("Unsupported transaction type!");
//            }
//        }
//        return transactionCount;
//    }
//
//    @Override
//    public Triple<Integer, Long, Double> call() throws Exception {
//        this.transaction = new Transactions(this.index);
//        Triple<Integer, Long, Double> result = null;
//
//        if (this.index <= 0) {
//            Scanner sc = new Scanner(System.in);
//            readTransaction(sc);
//        } else {
//            try {
//                // read file from xact folder
//                Scanner sc = new Scanner(new BufferedReader(
//                        new FileReader("project-files2/xact-files/" + this.index + ".txt")));
//                long st = System.currentTimeMillis();
//                long transactionCount = readTransaction(sc);
//                long et = System.currentTimeMillis();
//                double executionTime = (et - st) * 1.0 / 1000;
//
//                result = new Triple<>(this.index, transactionCount, executionTime);
//            } catch (FileNotFoundException e) {
//                System.out.println("Read input file error for client thread with index " + index);
//                System.out.println(e.getMessage());
//            }
//        }
//
//        return result;
//    }
//}


public class Main2 {
    private static Transactions transaction;

    //Transactions transaction;
    public static void main(String[] args) {
        BufferedReader bf = null;
        try {
            FileReader fr = new FileReader(args[0]);
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
            switch (lineData[0]) {
                case "N":
                    // new order transaction
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
                    // payment transaction
                    transaction.processPayment(Integer.parseInt(lineData[1]), Integer.parseInt(lineData[2]), Integer.parseInt(lineData[3]), Float.parseFloat(lineData[4]));
                    break;
                case "D":
                    // delivery transaction
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
        }
    }

    public static void setTransaction(Transactions transaction) {
        Main2.transaction = transaction;
    }
}