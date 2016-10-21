package zw.wormsleep.tools.etl.database;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ConnectionPool {
    static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    static final String DRIVER_CLASS = "driverClass";
    static final String JDBC_URL = "jdbcUrl";
    static final String USER = "user";
    static final String PASSWORD = "password";

    // 当连接池满了 c3p0 一次性获取的连接数（默认值：3）
    static final String ACQUIRE_INCREMENT = "acquireIncrement";
    // 定义当从数据库获取新连接时 c3p0 尝试的次数（默认值：30）
    static final String ACQUIRE_RETRY_ATTEMPTS = "acquireRetryAttempts";
    // c3p0 获取间隔的时间（默认值：1000 毫秒）
    static final String ACQUIRE_RETRY_DELAY = "acquireRetryDelay";
    // c3p0 是异步操作的缓慢的 JDBC 操作通过帮助进程完成。扩展这些操作可以有效的提升性能，通过多线程实现多个操作同时被执行。（默认值：3）
    static final String NUM_HELPER_THREADS = "numHelperThreads";
    // 连接池初始化时创建的连接数（默认值：3）
    static final String INITIAL_POOL_SIZE = "initialPoolSize";
    // 连接池保持的最小连接数（默认值：3）
    static final String MIN_POOL_SIZE = "minPoolSize";
    // 连接池拥有的最大连接数（默认值：15）
    static final String MAX_POOL_SIZE = "maxPoolSize";
    // 间隔多少秒检查所有连接池中的空闲连接（默认值：0 表示不检查）
    static final String IDLE_CONNECTION_TEST_PERIOD = "idleConnectionTestPeriod";
    // 当连接池用完时客户端等待获取新连接的时间，超时后将抛出 SQLException （默认值：0 毫秒。表示无限等待）
    static final String CHECKOUT_TIMEOUT = "checkoutTimeout";
    // 最大空闲时间,多少秒内未使用则连接被丢弃。（默认值：0 表示永不丢弃）
    static final String MAX_IDLE_TIME = "maxIdleTime";
    // 测试连接的语句，若不设置则通过 getTables() 获取 MetaData 其速度将远慢于设定该语句的情况。（默认值：null）（推荐设置）
    static final String PREFERRED_TEST_QUERY = "preferredTestQuery";
    //
    static final String TEST_CONNECTION_ON_CHECKOUT = "testConnectionOnCheckout";
    //
    static final String TEST_CONNECTION_ON_CHECKIN = "testConnectionOnCheckin";

    private static Map<String, ComboPooledDataSource> pool = new HashMap<String, ComboPooledDataSource>();

    private ConnectionPool() {
    }

    private static ComboPooledDataSource create(Map<String, String> poolConfig)
            throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();

        // 必须配置的项目
        cpds.setDriverClass(poolConfig.get(DRIVER_CLASS));
        cpds.setJdbcUrl(poolConfig.get(JDBC_URL));
        cpds.setUser(poolConfig.get(USER));
        cpds.setPassword(poolConfig.get(PASSWORD));

        // 可选（或者拥有默认值的项目）
        if (poolConfig.get(INITIAL_POOL_SIZE) != null)
            cpds.setInitialPoolSize(Integer.valueOf(poolConfig.get(INITIAL_POOL_SIZE)));
        if (poolConfig.get(MIN_POOL_SIZE) != null)
            cpds.setMinPoolSize(Integer.valueOf(poolConfig.get(MIN_POOL_SIZE)));
        if (poolConfig.get(MAX_POOL_SIZE) != null)
            cpds.setMaxPoolSize(Integer.valueOf(poolConfig.get(MAX_POOL_SIZE)));
        if (poolConfig.get(ACQUIRE_INCREMENT) != null)
            cpds.setAcquireIncrement(Integer.valueOf(poolConfig.get(ACQUIRE_INCREMENT)));
        if (poolConfig.get(ACQUIRE_RETRY_ATTEMPTS) != null)
            cpds.setAcquireRetryAttempts(Integer.valueOf(poolConfig.get(ACQUIRE_RETRY_ATTEMPTS)));
        if (poolConfig.get(ACQUIRE_RETRY_DELAY) != null)
            cpds.setAcquireRetryAttempts(Integer.valueOf(poolConfig.get(ACQUIRE_RETRY_DELAY)));
        if (poolConfig.get(IDLE_CONNECTION_TEST_PERIOD) != null)
            cpds.setIdleConnectionTestPeriod(Integer.valueOf(poolConfig.get(IDLE_CONNECTION_TEST_PERIOD)));
        if (poolConfig.get(MAX_IDLE_TIME) != null)
            cpds.setMaxIdleTime(Integer.valueOf(poolConfig.get(MAX_IDLE_TIME)));
        if (poolConfig.get(CHECKOUT_TIMEOUT) != null)
            cpds.setCheckoutTimeout(Integer.valueOf(poolConfig.get(CHECKOUT_TIMEOUT)));
        if (poolConfig.get(NUM_HELPER_THREADS) != null)
            cpds.setCheckoutTimeout(Integer.valueOf(poolConfig.get(NUM_HELPER_THREADS)));
        if (poolConfig.get(PREFERRED_TEST_QUERY) != null)
            cpds.setPreferredTestQuery(poolConfig.get(PREFERRED_TEST_QUERY));
        if (poolConfig.get(TEST_CONNECTION_ON_CHECKOUT) != null)
            cpds.setTestConnectionOnCheckout(Boolean.parseBoolean(poolConfig.get(TEST_CONNECTION_ON_CHECKOUT)));
        if (poolConfig.get(TEST_CONNECTION_ON_CHECKIN) != null)
            cpds.setTestConnectionOnCheckin(Boolean.parseBoolean(poolConfig.get(TEST_CONNECTION_ON_CHECKIN)));

        return cpds;

    }

    public static Connection getConnection(String database,
                                           Map<String, String> poolConfig) throws PropertyVetoException, SQLException {

        ComboPooledDataSource cpds = pool.get(database);

        if (cpds == null) {
            cpds = create(poolConfig);
            pool.put(database, cpds);
        }

        logger.info("@@@ 数据库节点：{} 从连接池获取连接成功！\n" +
                        "连接池状态 - 总连接数：{} - 使用：{} - 空闲：{}",
                database, cpds.getNumConnections(), cpds.getNumBusyConnections(), cpds.getNumIdleConnections());

        return cpds.getConnection();

    }

    public static void release(String database) {
        ComboPooledDataSource cpds = pool.get(database);
        if (cpds != null) {
            cpds.close();
        }
    }

    public static void releaseAll() {
        for (String key : pool.keySet()) {
            pool.get(key).close();
        }
    }

}
