import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;


class ClientThread implements Callable<Triple<Integer, Long, java.lang.Double>> {
    private int index;
    private Transactions transaction;

    ClientThread(int index) {
        this.index = index;
    }

    private long readTransaction(Scanner sc) {
        int transactionCount = 0;
        while (sc.hasNext()) {
            transactionCount++;
            String[] instruction = sc.nextLine().split(",");
            switch (instruction[0]) {
                case "N":
                    // new order transaction
                    int itemCount = Integer.parseInt(instruction[instruction.length - 1]);

                    List<List<Integer>> itemOrders = new ArrayList<>();
                    for (int i = 0; i < itemCount; i++) {
                        String[] orderString = sc.nextLine().split(",");
                        List<Integer> order = new ArrayList<>();
                        for (String s : orderString) {
                            order.add(Integer.parseInt(s));
                        }
                        itemOrders.add(order);
                    }

                    transaction.processOrder(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), itemOrders);
                    break;
                case "P":
                    // payment transaction
                    transaction.processPayment(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), Float.parseFloat(instruction[4]));
                    break;
                case "D":
                    // delivery transaction
                    transaction.processDelivery(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]));
                    break;
                case "O":
                    transaction.processOrderStatus(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]));
                    break;
                case "S":
                    transaction.processStockLevel(Integer.parseInt(instruction[1]), Integer.parseInt(instruction[2]), Integer.parseInt(instruction[3]), Integer.parseInt(instruction[4]));
                    break;
                case "I":
                    transaction.popularItem(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]));
                    break;
                case "T":
                    transaction.topBalance();
                    break;
                case "R":
                    transaction.processRelatedCustomer(Integer.parseInt(instruction[1]),
                            Integer.parseInt(instruction[2]),
                            Integer.parseInt(instruction[3]));
                default:
                    System.out.println("Unsupported transaction type!");
            }
        }
        return transactionCount;
    }

    @Override
    public Triple<Integer, Long, java.lang.Double> call() throws Exception {
        this.transaction = new Transactions(this.index);
        Triple<Integer, Long, java.lang.Double> result = null;

        if (this.index <= 0) {
            Scanner sc = new Scanner(System.in);
            readTransaction(sc);
        } else {
            try {
                // read file from xact folder
                Scanner sc = new Scanner(new BufferedReader(
                        new FileReader("project-files2/xact-files/" + this.index + ".txt")));
                long st = System.currentTimeMillis();
                long transactionCount = readTransaction(sc);
                long et = System.currentTimeMillis();
                double executionTime = (et - st) * 1.0 / 1000;

                result = new Triple<>(this.index, transactionCount, executionTime);
            } catch (FileNotFoundException e) {
                System.out.println("Read input file error for client thread with index " + index);
                System.out.println(e.getMessage());
            }
        }

        return result;
    }
}


public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter number of clients : ");
        int clientCount = sc.nextInt();

        ExecutorService executor = Executors.newFixedThreadPool(
                Math.max(1, clientCount));
        List<Future<Triple<Integer, Long, java.lang.Double>>> futureMeasurements = new ArrayList<>();

        if (clientCount <= 0) {
            // read from command line
            executor.submit(new ClientThread(0));
        } else {
            // read from xact files
            for (int i = 1; i <= clientCount; i++) {
                Future<Triple<Integer, Long, java.lang.Double>> future = executor.submit(new ClientThread(i));
                futureMeasurements.add(future);
            }
        }

        // process promise
        Map<Integer, Triple<Integer, Long, java.lang.Double>> measurementMap = new HashMap<>();
        for (Future<Triple<Integer, Long, java.lang.Double>> future : futureMeasurements) {
            try {
                Triple<Integer, Long, java.lang.Double> tuple = future.get();
                measurementMap.put(tuple.first, tuple);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                // no point to output performance result if error occurs
                return;
            }
        }

        outputPerformanceResult(measurementMap);
        System.out.println("\nAll client have completed their transactions.");
    }

    // output performance result
    private static void outputPerformanceResult(Map<Integer, Triple<Integer, Long, java.lang.Double>> measurementMap) {
        int clientCount = measurementMap.size();
        String outFilePath = "performanceMeasurement.txt";

        try {
            PrintWriter out = new PrintWriter(outFilePath);

            // find minimum, average and maximum transaction throughput
            Triple<Integer, Long, java.lang.Double> minTuple = measurementMap.get(1);
            Triple<Integer, Long, java.lang.Double> maxTuple = measurementMap.get(1);
            double sumThroughput = 0;
            for (Triple<Integer, Long, java.lang.Double> tuple : measurementMap.values()) {
                double currentThroughput = tuple.second / tuple.third;
                sumThroughput += currentThroughput;
                if (currentThroughput < minTuple.second / minTuple.third) {
                    minTuple = tuple;
                } else if (currentThroughput > maxTuple.second / maxTuple.third) {
                    maxTuple = tuple;
                }
            }
            out.println("Average transaction throughput is: " + (sumThroughput / clientCount));
            out.println("Minimum transaction throughput is Client with index: " + minTuple.first
                    + " with throughput: " + (minTuple.second / minTuple.third));
            out.println("Maximum transaction throughput is Client with index: " + maxTuple.first
                    + " with throughput: " + (maxTuple.second / maxTuple.third));
            out.println(); // empty line

            // output each Client performance
            for (int i = 1; i <= clientCount; i++) {
                Triple<Integer, Long, java.lang.Double> tuple = measurementMap.get(i);
                int index = tuple.first;
                long transactionCount = tuple.second;
                double executionTime = tuple.third;
                out.println("Performance measure for client with index: " + index);
                out.println("Transaction count: " + transactionCount);
                out.println("Total transaction execution time: " + executionTime);
                out.println("Transaction throughput: " + (transactionCount / executionTime));
                out.println(); // empty line
            }
            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Output performance measure file error.");
            System.out.println(e.getMessage());
        }
    }
}

class Triple<T, U, V> {
    final T first;
    final U second;
    final V third;

    Triple(T first, U second, V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
