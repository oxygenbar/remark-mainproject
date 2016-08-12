package cn.oxygenbar.util.memcache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import cn.oxygenbar.constants.MemcachedKeyUtil;
import cn.oxygenbar.util.IniReader;
import cn.oxygenbar.util.Logs;
import cn.oxygenbar.util.StringUtil;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.auth.AuthInfo;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.utils.AddrUtil;

/**
 * @author oxygenBar
 * @date 2016/08/12
 */
public class XMemcachedUtil {
	// create a static client as most installs only need a single instance
	protected static MemcachedClient client;
	protected static XMemcachedClientBuilder builder;

	// set up connection pool once at class load
	static {
		try {
			IniReader r = IniReader.getInstance(0);
			String address = r.getValue("memcached", "address");
			String username = r.getValue("memcached", "username");
			String password = r.getValue("memcached", "password");

			builder = new XMemcachedClientBuilder(
					AddrUtil.getAddresses(address));
			if (!StringUtil.isEmpty(username) && !StringUtil.isEmpty(password)) {
				AuthInfo auth = AuthInfo.plain(username, password);
				builder.addAuthInfo(AddrUtil.getOneAddress(address), auth);
			}
			// Must use binary protocol
			builder.setCommandFactory(new BinaryCommandFactory());
			builder.setSessionLocator(new KetamaMemcachedSessionLocator());
			builder.setConnectionPoolSize(5);
			client = builder.build();
			client.setConnectTimeout(2000);
			client.setOpTimeout(4000);

			client.setPrimitiveAsString(true);
		} catch (IOException e) {
			Logs.geterrorLogger().error("[Exception] init memcache ERROR:", e);
		}
	}

	public static Object get(String key) {
		try {
			return client.get(key);
		} catch (Exception e) {
			Logs.geterrorLogger().error("get ERROR: key=" + key, e);
			return null;
		}
	}

	public static Object[] getMultiArray(String[] keys) {
		try {
			if (keys == null || keys.length == 0) {
				return null;
			}
			List<String> keylist = new ArrayList<String>();
			for (int i = 0; i < keys.length; i++) {
				keylist.add(keys[i]);
			}
			Map<String, Object> map = client.get(keylist);
			Object[] obj = new Object[keys.length];
			for (int i = 0; i < keys.length; i++) {
				obj[i] = map.get(keys[i]);
			}
			return obj;
		} catch (Exception e) {
			String tmp_key = "";
			if (keys != null) {
				tmp_key = Arrays.toString(keys);
			}
			Logs.geterrorLogger().error("getMultiArray ERROR: keys=" + tmp_key,
					e);
			return null;
		}
	}

	/**
	 * 放入
	 * 
	 */
	public static void put(String key, Object obj) {
		try { // 默认一天过期
			put(key, obj, 0);
		} catch (Exception e) {
			Logs.geterrorLogger().error("put(,) exception: key=" + key, e);
		}

	}

	public static void put(String key, Object obj, long exp) {
		try {
			if (obj != null) {
				int e = (int) (exp / 1000);
				client.set(key, e, obj);
			}
		} catch (Exception e) {
			Logs.geterrorLogger().error("put(,,) exception: key=" + key, e);
		}

	}

	/**
	 * 删除
	 */
	public static void remove(String key) {
		try {
			client.delete(key);
		} catch (Exception e) {
			Logs.geterrorLogger().error("remove exception: key=" + key, e);
		}
	}

	public static void clear() {
		try {
			client.flushAll();
		} catch (Exception e) {
			Logs.geterrorLogger().error("clear exception", e);
		}
	}

	/**
	 * 锁定一个key<br>
	 * exp小于1000时直接返回false
	 * 
	 * @param lockKey
	 * @return exp 毫秒
	 */
	public static boolean lock(String lockKey, int exp) {
		try {
			if (exp < 1000) {
				return false;
			}
			return client.add(lockKey, exp / 1000, true);
		} catch (Exception e) {
			Logs.geterrorLogger().error("lock exception: key=" + lockKey, e);
			return false;
		}
	}

	/**
	 * 锁定一个key
	 * 
	 * @param lockKey
	 * @return
	 */
	public static boolean lock(String lockKey) {
		return lock(lockKey, 1000);
	}

	/**
	 * 解锁key
	 * 
	 * @param lockKey
	 */
	public static void unlock(String lockKey) {
		try {
			client.delete(lockKey);
		} catch (Exception e) {
			Logs.geterrorLogger().error("unlock exception: key=" + lockKey, e);
		}
	}

	/**
	 * 
	 * @param key
	 * @param by
	 *            步长
	 * @param def
	 *            初始值
	 * @return
	 */
	public static long apiIncr(String key, long by, long def) {
		try {
			return client.incr(key, by, def, 2000, 20);
		} catch (Exception e) {
			Logs.geterrorLogger().error("apiIncr(,,) exception: key=" + key, e);
			return 0;
		}

	}

	/**
	 * 
	 * @param key
	 * @param by
	 *            步长
	 * @param def
	 *            初始值
	 * @param exp
	 *            过期时间
	 * @return
	 */
	public static long apiIncr(String key, long by, long def, int exp) {
		try {
			return client.incr(key, by, def, 2000, exp);
		} catch (Exception e) {
			Logs.geterrorLogger()
					.error("apiIncr(,,,) exception: key=" + key, e);
			return 0;
		}

	}

	public static long apiIncr(String key) {
		try {
			return client.incr(key, 1, 0);
		} catch (Exception e) {
			Logs.geterrorLogger().error("apiIncr() exception: key=" + key, e);
			return 0;
		}
	}

	public static long getApiIncr(String key) {
		try {
			Object obj = client.get(key);
			if (null != obj) {
				return StringUtil.toLong(obj.toString());
			}
			return -1;
		} catch (Exception e) {
			Logs.geterrorLogger().error("getApiIncr exception: key=" + key, e);
			return 0;
		}
	}

}

class LocalCache {
	private Object cacheValue;
	private long cacheTime;

	public static void main(String[] args) {
		// System.out.println(XMemcachedUtil.get(MemcachedKeyUtil.MONEY_RATE_LIST+"CNY"));
		List<String> abcList = new ArrayList<String>();
		abcList.add("lei");
		abcList.add("lei1");
		XMemcachedUtil.put("lei_test001", abcList, 1000 * 60);
		long start = System.currentTimeMillis();
		for (int i = 0; i <= 1000000; i++) {
			XMemcachedUtil.get("lei_test001");
		}
		System.out.println((System.currentTimeMillis()-start)/1000);
		// XMemcachedUtil.put("key_publish_coin_total_onbank", 0,1);
		// XMemcachedUtil.put("nation_area_all", 0,1);

		// XMemcachedUtil.put(XMemcachedUtil.user, 0,1);
		//
		// List<String> abccc = (List<String>)XMemcachedUtil.get("lei_test001");

		// System.out.println(abccc);
		// System.out.println(XMemcachedUtil.get("key_publish_coin_total_onbank"));

		// System.out.println(XMemcachedUtil.get(MemcachedKeyUtil.INVITE_CODE_PREX+"1939473404@qq.com"));

		// System.out.println(XMemcachedUtil.get(MemcachedKeyUtil.BTC_TICKER_ALL));
		// System.out.println(XMemcachedUtil.get("btc_coinbase"));
		// [{btc_okcoin_usd=643.8}, {btc_bitfinex=646.62},
		// {btc_coinbase=650.52}, {btc_bitstamp=646.26}]
		// try {
		// Thread.sleep(30000);
		// } catch (Exception e) {
		// // TODO: handle exception
		// }

		// System.out.println(XMemcachedUtil.get(MemcachedKeyUtil.BTC_TICKER_ALL));
	}

	public Object getCacheValue() {
		return cacheValue;
	}

	public void setCacheValue(Object cacheValue) {
		this.cacheValue = cacheValue;
	}

	public long getCacheTime() {
		return cacheTime;
	}

	public void setCacheTime(long cacheTime) {
		this.cacheTime = cacheTime;
	}
}
