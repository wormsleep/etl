package zw.wormsleep.tools.etl.database;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class ConnectionPool {
	static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

	static final String DRIVER_CLASS = "driverClass";
	static final String JDBC_URL = "jdbcUrl";
	static final String USER = "user";
	static final String PASSWORD = "password";
	static final String MIN_POOL_SIZE = "minPoolSize";
	static final String MAX_POOL_SIZE = "maxPoolSize";
	static final String ACQUIRE_INCREMENT = "acquireIncrement";
	static final String INITIAL_POOL_SIZE = "initialPoolSize";
	static final String IDLE_CONNECTION_TEST_PERIOD = "idleConnectionTestPeriod";
	static final String CHECKOUT_TIMEOUT = "checkoutTimeout";
	static final String ACQUIRE_RETRY_ATTEMPTS = "acquireRetryAttempts";
	static final String TEST_CONNECTION_ON_CHECKOUT = "testConnectionOnCheckout";
	static final String PREFERRED_TEST_QUERY = "preferredTestQuery";

	private static Map<String, ComboPooledDataSource> pool = new HashMap<String, ComboPooledDataSource>();

	private ConnectionPool() {
	}

	private static ComboPooledDataSource create(Map<String, String> poolConfig)
			throws PropertyVetoException {
		ComboPooledDataSource cpds = new ComboPooledDataSource();

		cpds.setDriverClass(poolConfig.get(DRIVER_CLASS)); // 驱动器
		cpds.setJdbcUrl(poolConfig.get(JDBC_URL)); // 数据库url
		cpds.setUser(poolConfig.get(USER)); // 用户名
		cpds.setPassword(poolConfig.get(PASSWORD)); // 密码
		cpds.setInitialPoolSize(Integer.valueOf(poolConfig
				.get(INITIAL_POOL_SIZE))); // 初始化连接池大小
		cpds.setMinPoolSize(Integer.valueOf(poolConfig.get(MIN_POOL_SIZE))); // 最少连接数
		cpds.setMaxPoolSize(Integer.valueOf(poolConfig.get(MAX_POOL_SIZE))); // 最大连接数
		cpds.setAcquireIncrement(Integer.valueOf(poolConfig
				.get(ACQUIRE_INCREMENT))); // 连接数的增量
		cpds.setIdleConnectionTestPeriod(Integer.valueOf(poolConfig
				.get(IDLE_CONNECTION_TEST_PERIOD))); // 测连接有效的时间间隔
		cpds.setCheckoutTimeout(Integer.valueOf(poolConfig
				.get(CHECKOUT_TIMEOUT)));
		cpds.setAcquireRetryAttempts(Integer.valueOf(poolConfig
				.get(ACQUIRE_RETRY_ATTEMPTS)));
		cpds.setTestConnectionOnCheckout(poolConfig.get(
				TEST_CONNECTION_ON_CHECKOUT).equalsIgnoreCase("true") ? true
				: false);
		cpds.setPreferredTestQuery(poolConfig.get(PREFERRED_TEST_QUERY));

		return cpds;

	}

	public static Connection getConnection(String database,
			Map<String, String> poolConfig) throws PropertyVetoException, SQLException {
		
		ComboPooledDataSource cpds = (ComboPooledDataSource) pool.get(database);
		
		if(cpds == null) {
			cpds = create(poolConfig);
			pool.put(database, cpds);
		}
		
		return cpds.getConnection();
		
	}
	
	public static void release(String database) {
		ComboPooledDataSource cpds = (ComboPooledDataSource) pool.get(database);
		if(cpds != null) {
			cpds.close();
		}
	}
	
	public static void releaseAll() {
		for(String key : pool.keySet()) {
			pool.get(key).close();
		}
	}

}
