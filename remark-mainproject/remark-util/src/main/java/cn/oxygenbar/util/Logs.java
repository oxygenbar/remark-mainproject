package cn.oxygenbar.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Logs {

    private final static Log infolog;
    private final static Log slowlog;
    private final static Log errorlog;
    private final static Log dblog;

    static {
        infolog = LogFactory.getLog(Logs.class);
        slowlog = LogFactory.getLog("slowLog");
        errorlog = LogFactory.getLog("errorLog");
        dblog = LogFactory.getLog("dbLog");
    }

    public static Log getinfoLogger() {
        return infolog;
    }

    public static Log getslowLogger() {
        return slowlog;
    }

    public static Log geterrorLogger() {
        return errorlog;
    }

    public static Log getDblogger() {
        return dblog;
    }

    public static boolean ifLog(int n) {
        return System.currentTimeMillis() % n == 1;
    }

}
