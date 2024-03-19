import com.beust.jcommander.JCommander;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Benchmark extends Thread {

    private static Options options = new Options();

    private static boolean started, stopped;
    private static int n, aborted;
    private static long total;
    private static Map<TransactionType, Long[]> rtPerType = new HashMap<>();


    public synchronized static void startBench() {
        started = true;

        System.out.println("Started!");
    }

    public synchronized static void logTransaction(long tr, boolean success, TransactionType type) {
        if (started && !stopped) {
            n++;
            total += tr;
            if (!success) {
                aborted +=1;
            }
            else {
                if (!rtPerType.containsKey(type)) {
                    rtPerType.put(type, new Long[]{tr, 1L});
                }
                else {
                    rtPerType.get(type)[0] += tr;
                    rtPerType.get(type)[1] += 1;
                }
            }
        }
    }

    public synchronized static boolean isStopped() {
        return stopped;
    }

    public synchronized static void stopBench() {
        stopped = true;

        System.out.println("Response time per function (ms)");
        System.out.println("-------------------------------");
        rtPerType.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.println(e.getKey() + " = " + e.getValue()[0] / (e.getValue()[1] * 1e6d)));

        System.out.println("\nOverall metrics");
        System.out.println("---------------");
        System.out.println("throughput (txn/s) = "+(n/((double) options.runtime)));
        System.out.println("response time (ms) = "+(total/(n*1e6d)));
        System.out.println("abort rate (%) = "+(aborted*100/((double)n)));

    }

    public void run() {
        try {
            Connection c = DriverManager.getConnection(options.database, options.user, options.passwd);
            Workload wl = new Workload(c);
            while(!isStopped()) {
                long before = System.nanoTime();
                boolean success = true;
                TransactionType type = null;
                try {
                    type =  wl.transaction();
                } catch(SQLException e) {
                    // check if it is an isolation or uniqueness-related exception
                    // make sure other exceptions are shown
                    if (e.getSQLState().startsWith("40") || e.getSQLState().startsWith("23")) {
                        try {
                            c.rollback();
                        } catch(Exception ignored) { }
                        success = false;
                    } else
                        throw e;
                } finally {
                    long after = System.nanoTime();
                    logTransaction(after - before, success, type);
                }
            }
            c.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void execute() throws Exception {
        Benchmark[] r = new Benchmark[options.clients];
        for(int i=0; i<r.length; i++)
            r[i] = new Benchmark();
        for(int i=0; i<r.length; i++)
            r[i].start();
        Thread.sleep(options.warmup * 1000L);
        startBench();
        Thread.sleep(options.runtime * 1000L);
        stopBench();
        for(int i=0; i<r.length; i++)
            r[i].join();
    }

    public static void main(String[] args) throws Exception {

        JCommander parser = JCommander.newBuilder()
                .addObject(options)
                .build();
        try {
            parser.parse(args);
            if (options.help) {
                parser.usage();
                return;
            }
        } catch(Exception e) {
            parser.usage();
            return;
        }

        execute();
    }

}
