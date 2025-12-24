package paxos.utils;

public class PrintHelper {
 
    private static final boolean DEBUG =  Boolean.parseBoolean(System.getProperty("debugLogs", "false"));
    private static final boolean PERFORMANCE_MODE = Boolean.parseBoolean(System.getProperty("performanceMode", "false"));

    public static boolean isDebugEnabled() {
        return DEBUG;
    }

    public static void debugf(String fmt, Object... args) {
        if (!isDebugEnabled()) return;
        System.out.printf(fmt + "%n", args);
    }
}
