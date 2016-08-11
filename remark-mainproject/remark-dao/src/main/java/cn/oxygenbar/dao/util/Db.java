package cn.oxygenbar.dao.util;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import cn.oxygenbar.constants.MessageCode;
import cn.oxygenbar.util.DateUtil;
import cn.oxygenbar.util.IniReader;
import cn.oxygenbar.util.Logs;
import cn.oxygenbar.util.StringUtil;

public class Db {

	// 保存现在线程使用的是哪个服务器
	private static final ThreadLocal<Integer> serverPool = new ThreadLocal<Integer>();

	private static final ThreadLocal<Connection> connPooMain = new ThreadLocal<Connection>();
	private static final ThreadLocal<Connection> connPoolBBS = new ThreadLocal<Connection>();

	// 数据库连接资源
	public static Map<String, DataSource> dsmap = new HashMap<String, DataSource>();

	private static String[] SERVERS_NAME = { "SERVER_NOTSET", "SERVER_MAIN",
			"SERVER_BBS" };

	public static final int SERVER_NOTSET = 0; // 默认使用主库连接
	public static final int SERVER_MAIN = 1; // 使用主库连接
	public static final int SERVER_BBS = 2; // bbs库连接

	// 主库表名
	private static Set<String> tablename_main = new HashSet<String>();
	// 辅库表名
	private static Set<String> bbs_set = new HashSet<String>();

	static {

		// 装载表名
		for (String str : DbTableNameUtil.table_main) {
			tablename_main.add(str);
		}
		for (String str : DbTableNameUtil.bbs_server_table) {
			bbs_set.add(str);
		}

		try {
			InitialContext ctx = new InitialContext();
			// 读取配置文件pay.ini
			IniReader reader = IniReader.getInstance(0);
			Properties p = reader.getSection("database");
			if (p != null) {
				Enumeration<Object> keys = p.keys();
				Map<String, DataSource> _dsmap = new HashMap<String, DataSource>();
				while (keys.hasMoreElements()) {
					String key = (String) keys.nextElement();
					String value = p.getProperty(key);
					DataSource dsW = _dsmap.get(value);
					if (dsW == null) {
						dsW = (DataSource) ctx.lookup(value);
						_dsmap.put(value, dsW);
					}
					dsmap.put(key, dsW);
				}
			}

		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		}
	}

	public static ConnectionBean getConn(int server) {// 写或有读有写
		try {
			ConnectionBean connectionBean;

			if (server == SERVER_MAIN || server == SERVER_NOTSET) {
				Connection connMain = (Connection) connPooMain.get();
				if (connMain == null || connMain.isClosed()) {
					connectionBean = getConnMain();
					connPooMain.set(connectionBean.getConn());
				} else {
					connectionBean = new ConnectionBean();
					connectionBean.setConn(connMain);
					connectionBean.setTime(0);
				}

				return connectionBean;
			}
			if (SERVER_BBS == server) { // bbs库，不区分读写
				Connection connBBS = (Connection) connPoolBBS.get();
				if (connBBS == null || connBBS.isClosed()) {
					connectionBean = getConnBBS();
					connPoolBBS.set(connectionBean.getConn());
				} else {
					connectionBean = new ConnectionBean();
					connectionBean.setConn(connBBS);
					connectionBean.setTime(0);
				}
				return connectionBean;
			}
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		}

		return null;
	}

	public static Connection getConn_notry(int server) {// 写或有读有写
		try {
			if (server == SERVER_MAIN || server == SERVER_NOTSET) {
				Connection connMain = (Connection) connPooMain.get();
				return connMain;
			}

			if (SERVER_BBS == server) { // bbs库，不区分读写
				Connection connBBS = (Connection) connPoolBBS.get();
				return connBBS;
			}
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		}

		return null;
	}

	public static ConnectionBean getConnMain() {// 纯写或有读有
		try {
			DataSource dsMain = dsmap.get("datasourcepay");
			ConnectionBean connectionBean = new ConnectionBean();
			long startTime = System.currentTimeMillis();
			Connection conn = dsMain.getConnection();
			connectionBean.setConn(conn);
			connectionBean.setTime(System.currentTimeMillis() - startTime);
			return connectionBean;
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		}
		return null;
	}

	public static ConnectionBean getConnBBS() { // BBS库
		try {
			DataSource dsBbsDb = dsmap.get("datasourcebbs");
			ConnectionBean connectionBean = new ConnectionBean();
			long startTime = System.currentTimeMillis();
			Connection conn = dsBbsDb.getConnection();
			connectionBean.setConn(conn);
			connectionBean.setTime(System.currentTimeMillis() - startTime);
			return connectionBean;
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		}
		return null;
	}

	private static int getServer(String sql, Object[] parm) {
		int server = SERVER_NOTSET;
		String[] tablename = DbTableNameUtil.getTableNameBySql(sql);

		if (tablename == null || tablename.length < 1) {
			return SERVER_NOTSET;
		}
		for (int i = 0; i < tablename.length; i++) {
			String tmp = tablename[i];
			tmp = StringUtil.replaceStr(tmp, "`", "").trim();
			if (tablename_main.contains(tmp)) {
				server = SERVER_MAIN;
				break;
			} else if (bbs_set.contains(tmp)) {
				server = SERVER_BBS;
				break;
			}
		}
		return server;
	}

	public static long executeUpdate(String sql) {
		return executeUpdate(sql, null, SERVER_NOTSET);
	}

	public static long executeUpdate(String sql, Object[] parm) {
		return executeUpdate(sql, parm, SERVER_NOTSET);
	}

	public static long executeUpdate(String sql, Object[] parm, int server) {
		if (!DbTableNameUtil.checkSql(sql)) {
			Logs.geterrorLogger().error("危险的sql不予执行 ： " + sql);
			return 0;
		}

		PreparedStatement pstmt = null;
		Connection conn = null;
		if (server == SERVER_NOTSET) {
			Integer s = (Integer) serverPool.get();
			if (s == null) {
				server = getServer(sql, parm);
			} else {
				server = s.intValue();
			}
		}

		try {
			ConnectionBean connectionBean = getConn(server);
			conn = connectionBean.getConn();

			if (sql == null) {
				return 0;
			}
			boolean ifinsert = false;
			if (sql.indexOf("insert") > -1 || sql.indexOf("INSERT") > -1)
				ifinsert = true;
			if (ifinsert == true) {
				pstmt = conn.prepareStatement(sql,
						Statement.RETURN_GENERATED_KEYS);
			} else {
				pstmt = conn.prepareStatement(sql);
			}
			if (parm != null && parm.length > 0) {
				for (int i = 0; i < parm.length; i++) {
					pstmt.setObject(i + 1, parm[i]);
				}
			}

			// if (PayConstants.SHOW_SQL) {
			// String targetSql = getPreparedSQL(sql, parm);
			// Logs.getDblogger().info(targetSql);
			// }

			if (ifinsert) {
				long result = pstmt.executeUpdate();
				ResultSet keys = pstmt.getGeneratedKeys();
				long id = 0;
				if (keys.next()) {
					id = keys.getLong(1);
				}
				if (id != 0) {
					result = id;
				}
				return result;
			} else {
				if (DbTableNameUtil.filterSql(sql)) {
					return pstmt.executeUpdate();
				} else {
					throw new Exception("delete or update sql is error : "
							+ sql);
				}
			}

		} catch (Exception ex) {
			String em = ex.getMessage();
			if (em != null && em.indexOf("Duplicate entry") >= 0) {
				String param = "param : ";
				if (parm != null) {
					for (int i = 0; i < parm.length; i++) {
						param += parm[i] + ",";
					}
				}
				if (sql != null && sql.indexOf("insert into btc_user") < 0) {
					Logs.geterrorLogger().error(
							"Duplicate entry ,error sql:" + sql + " param :"
									+ param);
				}
			} else {
				String[] tbName = DbTableNameUtil.getTableNameBySql(sql);
				String tb = "";
				if (tbName != null) {
					for (String string : tbName) {
						tb += string + ",";
					}
				} else {
					tb = "NULL table name";
				}
				String param = "param : ";
				if (parm != null) {
					for (int i = 0; i < parm.length; i++) {
						param += parm[i] + ",";
					}
				}
				Logs.geterrorLogger().error(
						"db.executeUpdate error \r\n" + sql + "\r\ntable name "
								+ tb + "\r\n" + SERVERS_NAME[server] + "\r\n"
								+ " " + param + "\r\n", ex);
			}
			return -1;
		} finally {
			closePstmt(pstmt);
			releaseConnection();
		}
	}

	public static long executeUpdate_notry(String sql, Object[] parm)
			throws Exception {
		return executeUpdate_notry(sql, parm, SERVER_NOTSET);
	}

	public static long executeUpdate_notry(String sql, Object[] parm, int server)
			throws Exception {
		if (!DbTableNameUtil.checkSql(sql)) {
			Logs.geterrorLogger().error("危险的sql不予执行 ： " + sql);
			return 0;
		}
		PreparedStatement pstmt = null;
		Connection conn = null;
		if (server == SERVER_NOTSET) {
			Integer s = (Integer) serverPool.get();
			if (s == null) {
				server = getServer(sql, parm);
			} else {
				server = s.intValue();
			}
		}
		try {
			conn = getConn_notry(server);

			if (sql == null) {
				return 0;
			}
			boolean ifinsert = false;
			if (sql.indexOf("insert") > -1 || sql.indexOf("INSERT") > -1)
				ifinsert = true;
			if (ifinsert == true) {
				pstmt = conn.prepareStatement(sql,
						Statement.RETURN_GENERATED_KEYS);
			} else {
				pstmt = conn.prepareStatement(sql);
			}
			if (parm != null && parm.length > 0) {
				for (int i = 0; i < parm.length; i++) {
					pstmt.setObject(i + 1, parm[i]);
				}
			}

			// if (PayConstants.SHOW_SQL) {
			// String targetSql = getPreparedSQL(sql, parm);
			// Logs.getDblogger().info(targetSql);
			// }

			if (ifinsert) {
				long result = pstmt.executeUpdate();
				ResultSet keys = pstmt.getGeneratedKeys();
				long id = 0;
				if (keys.next()) {
					id = keys.getLong(1);
				}
				if (id != 0) {
					result = id;
				}
				return result;
			} else {
				if (DbTableNameUtil.filterSql(sql)) {
					return pstmt.executeUpdate();
				} else {
					throw new Exception("delete or update sql is error : "
							+ sql);
				}
			}

		} finally {
			closePstmt(pstmt);
		}
	}

	/**
	 * 批量更新
	 * 
	 * 用户表不能批量更新,因为所操作的记录在当前的连接里可能找不到.
	 * 
	 * @param sql
	 * @param parmList
	 * @return
	 */
	public static boolean executeBatchUpdate(String sql, List<Object[]> parmList) {
		if (!DbTableNameUtil.checkSql(sql)) {
			Logs.geterrorLogger().error("危险的sql不予执行 ： " + sql);
			return false;
		}

		PreparedStatement pstmt = null;
		Connection conn = null;
		boolean b = false;
		try {
			int server = getServer(sql, null);
			ConnectionBean connectionBean = getConn(server);
			conn = connectionBean.getConn();

			pstmt = conn.prepareStatement(sql);
			if (parmList != null && parmList.size() > 0) {
				for (int i = 0; i < parmList.size(); i++) {
					Object[] parm = parmList.get(i);
					if (parm != null && parm.length > 0) {
						for (int j = 0; j < parm.length; j++) {
							pstmt.setObject(j + 1, parm[j]);
						}
						pstmt.addBatch();
					}
				}
			}
			int[] num = pstmt.executeBatch();
			if (num != null && num.length > 0) {
				b = true;
			}
		} catch (Exception ex) {
			Logs.geterrorLogger().error("db.executeBatchUpdate error", ex);
			Logs.geterrorLogger().error("error sql:" + sql);
		} finally {
			closePstmt(pstmt);
			releaseConnection();
		}
		return b;
	}

	public static List<Map<String, Object>> executeQuery(String sql) {
		return executeQuery(sql, null);
	}

	public static List<Map<String, Object>> executeQuery(String sql,
			Object[] parm) {
		return executeQuery(sql, parm, SERVER_NOTSET);
	}

	public static List<Map<String, Object>> executeQuery(String sql,
			Object[] parm, int server) {
		if (!DbTableNameUtil.checkSql(sql)) {
			Logs.geterrorLogger().error("危险的sql不予执行 ： " + sql);
			return null;
		}
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSet res = null;
		if (server == SERVER_NOTSET) {
			Integer s = (Integer) serverPool.get();
			if (s == null) {
				server = getServer(sql, parm);
			} else {
				server = s.intValue();
			}
		}
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			long start = System.currentTimeMillis();
			ConnectionBean connectionBean = getConn(server);
			conn = connectionBean.getConn();
			long conntime = System.currentTimeMillis() - start;

			if (sql == null) {
				// error.error("in sql : " + tmpSql);
				return null;
			}
			pstmt = conn.prepareStatement(sql);
			if (parm != null && parm.length > 0) {
				for (int i = 0; i < parm.length; i++) {
					if (parm[i] == null) {
						break;
					}
					pstmt.setObject(i + 1, parm[i]);
				}
			}
			res = pstmt.executeQuery();

			long end = System.currentTimeMillis();
			StringBuffer param = new StringBuffer("param : ");
			if (parm != null) {
				for (int i = 0; i < parm.length; i++) {
					param.append(parm[i]).append(",");
				}
			}
			if ((end - start) > 2000) {
				Logs.getslowLogger().error(
						"server:" + SERVERS_NAME[server] + ",totaltime:"
								+ (end - start) + ",conntime:" + conntime
								+ ",connDbtime:" + connectionBean.getTime()
								+ ",longtimesql:" + sql + "&"
								+ param.toString());
			}
			if (MessageCode.SHOW_SQL) {
				Logs.getDblogger().error(
						"server:" + SERVERS_NAME[server] + ",totaltime:"
								+ (end - start) + ",conntime:" + conntime
								+ ",connDbtime:" + connectionBean.getTime()
								+ ",longtimesql:" + sql + "&"
								+ param.toString());
			}
			if (res != null) {
				int columnCount = res.getMetaData().getColumnCount();
				while (res.next()) {
					Map<String, Object> resultRow = new HashMap<String, Object>();
					for (int i = 1; i <= columnCount; i++) {
						resultRow.put(res.getMetaData().getColumnLabel(i),
								res.getObject(i));
					}
					result.add(resultRow);
					resultRow = null;
				}
			}
			return result;

		} catch (Exception ex) {
			String debug = "##DEBUG:";
			String[] tableName = DbTableNameUtil.getTableNameBySql(sql);
			for (String str : tableName)
				debug += str + "\r\n";
			String param = "param : ";
			if (parm != null) {
				for (int i = 0; i < parm.length; i++) {
					param += parm[i] + ",";
				}
			}
			Logs.geterrorLogger().error(
					debug + "error sql:" + sql + "server is:"
							+ SERVERS_NAME[server]
							+ "\r\ndb.executeQuery error , " + param, ex);
			return null;
		} finally {
			closeRes(res);
			closePstmt(pstmt);
			releaseConnection();
		}
	}

	public static List<Map<String, Object>> executeQuery_notry(String sql,
			Object[] parm) throws SQLException {
		return executeQuery_notry(sql, parm, SERVER_NOTSET);
	}

	public static List<Map<String, Object>> executeQuery_notry(String sql,
			Object[] parm, int server) throws SQLException {
		// String tmpSql = sql;
		if (!DbTableNameUtil.checkSql(sql)) {
			Logs.geterrorLogger().error("危险的sql不予执行 ： " + sql);
			return null;
		}
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSet res = null;
		if (server == SERVER_NOTSET) {
			Integer s = (Integer) serverPool.get();
			if (s == null) {
				server = getServer(sql, parm);
			} else {
				server = s.intValue();
			}
		}
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			long start = System.currentTimeMillis();
			conn = getConn_notry(server);
			long conntime = System.currentTimeMillis() - start;
			if (conn == null || conn.isClosed()) {
				Logs.geterrorLogger().error(
						"conn == null||conn.isClosed()" + sql);
				return null;
			}
			if (sql == null) {
				// error.error("in sql : " + tmpSql);
				return null;
			}
			pstmt = conn.prepareStatement(sql);
			if (parm != null && parm.length > 0) {
				for (int i = 0; i < parm.length; i++) {
					if (parm[i] == null) {
						break;
					}
					pstmt.setObject(i + 1, parm[i]);
				}
			}
			res = pstmt.executeQuery();

			// if (PayConstants.SHOW_SQL) {
			// String targetSql = getPreparedSQL(sql, parm);
			// Logs.getDblogger().info(targetSql);
			// }

			long end = System.currentTimeMillis();
			StringBuffer param = new StringBuffer("param : ");
			if (parm != null) {
				for (int i = 0; i < parm.length; i++) {
					param.append(parm[i]).append(",");
				}
			}
			if ((end - start) > 2000) {
				Logs.getslowLogger().error(
						"server:" + SERVERS_NAME[server] + ",totaltime:"
								+ (end - start) + ",conntime:" + conntime
								+ ",longtimesql:" + sql + "&"
								+ param.toString());
			}
			if (MessageCode.SHOW_SQL) {
				Logs.getDblogger().error(
						"server:" + SERVERS_NAME[server] + ",totaltime:"
								+ (end - start) + ",conntime:" + conntime
								+ ",longtimesql:" + sql + "&"
								+ param.toString());
			}
			if (res != null) {
				int columnCount = res.getMetaData().getColumnCount();
				while (res.next()) {
					Map<String, Object> resultRow = new HashMap<String, Object>();
					for (int i = 1; i <= columnCount; i++) {
						resultRow.put(res.getMetaData().getColumnLabel(i),
								res.getObject(i));
					}
					result.add(resultRow);
					resultRow = null;
				}
			}
			return result;

		} finally {
			closeRes(res);
			closePstmt(pstmt);
		}
	}

	public static int tran_begin(String sql) throws SQLException {
		int server = getServer(sql, null);
		Connection conn = null;
		serverPool.set(server);
		ConnectionBean connectionBean = getConn(server);
		long start = System.currentTimeMillis();
		conn = connectionBean.getConn();
		long end = System.currentTimeMillis();
		conn.setAutoCommit(false);
		long time = end - start;

		if (time > 2000) {
			Logs.getslowLogger().error(
					"server:" + SERVERS_NAME[server] + ",totaltime:" + time
							+ ",conntime:" + time + ",connDbtime:"
							+ connectionBean.getTime() + ",longtimesql:" + sql);
		}
		if (MessageCode.SHOW_SQL) {
			Logs.getDblogger().error(
					"server:" + SERVERS_NAME[server] + ",totaltime:" + time
							+ ",conntime:" + time + ",connDbtime:"
							+ connectionBean.getTime() + ",longtimesql:" + sql);
		}
		return server;
	}

	public static void tran_commit(String sql) {
		Integer server = (Integer) serverPool.get();
		if (server == null) {
			server = getServer(sql, null);
		}
		Connection conn = null;
		try {
			conn = getConn_notry(server.intValue());
			serverPool.remove();
			conn.commit();
			conn.setAutoCommit(true);
			// userLog.error(Thread.currentThread().getName() +
			// ",tran_commit conn = " + conn.hashCode());
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		} finally {
			releaseConnection();
		}
	}

	public static void tran_rollback(String sql) {
		Integer server = (Integer) serverPool.get();
		if (server == null) {
			server = getServer(sql, null);
		}
		Connection conn = null;
		try {
			conn = getConn_notry(server.intValue());
			serverPool.remove();
			conn.rollback();
			conn.setAutoCommit(true);
			// userLog.error(Thread.currentThread().getName() +
			// ",tran_rollback conn = " + conn.hashCode());
			// Logs.geterrorLogger().warn("transaction rollback"+sql);
		} catch (Exception ex) {
			Logs.geterrorLogger().error(ex.getMessage(), ex);
		} finally {
			releaseConnection();
		}
	}

	public static void closePstmt(PreparedStatement pstmt) {
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (Exception ex) {
				Logs.geterrorLogger().error("close stmt error", ex);
			}
		}
	}

	public static void closeRes(ResultSet res) {
		if (res != null) {
			try {
				res.close();
			} catch (Exception ex) {
				Logs.geterrorLogger().error("close res error", ex);
			}
		}
	}

	public static void releaseConnection() {
		releaseConnection(connPooMain);
		releaseConnection(connPoolBBS);
		serverPool.remove();
	}

	private static void releaseConnection(ThreadLocal<Connection> connPool) {
		Connection conn = (Connection) connPool.get();
		if (conn != null) {
			try {
				if (!conn.isClosed()) {

					if (!conn.getAutoCommit()) {
						Logs.getinfoLogger().error("Db  AutoCommit is false");
					}

					conn.setAutoCommit(true);
					conn.close();
				}
				conn = null;
			} catch (Exception ex) {
				Logs.geterrorLogger().warn("", ex);
			}
		}
		connPool.set(null);
	}

	// 生成最终执行sql
	private static String getPreparedSQL(String sql, Object[] params) {
		try {
			// 1 如果没有参数，说明是不是动态SQL语句
			if (params == null || params.length == 0)
				return sql;
			int paramNum = params.length;
			// 2 如果有参数，则是动态SQL语句
			StringBuffer returnSQL = new StringBuffer();
			String[] subSQL = sql.split("\\?");
			for (int i = 0; i < paramNum; i++) {
				if (params[i] instanceof Date) {
					returnSQL
							.append(subSQL[i])
							.append(" '")
							.append(DateUtil.dateUtil2String((Date) params[i],
									"yyyy-MM-dd HH:mm:ss")).append("' ");
				} else {
					returnSQL.append(subSQL[i]).append(" '").append(params[i])
							.append("' ");
				}
			}
			if (subSQL.length > params.length) {
				returnSQL.append(subSQL[subSQL.length - 1]);
			}
			return returnSQL.toString();
		} catch (Exception e) {
			Logs.geterrorLogger().error("prepared sql error", e);
		}
		return sql;
	}

}

class ConnectionBean {
	private long time;
	private Connection conn;

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}