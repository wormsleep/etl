package zw.wormsleep.tools.etl.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.ETLLoader;
import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.LoadConfig;
import zw.wormsleep.tools.etl.database.DatabaseHelper;
import zw.wormsleep.tools.etl.database.PreparedStatementPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class DatabasePlusLoader implements ETLLoader {

    private static final Integer BATCH_SIZE = 200;
    final Logger logger = LoggerFactory.getLogger(DatabaseLoader.class);
    private String businessType;
    private LoadConfig loadConfig;


    public DatabasePlusLoader(String businessType, LoadConfig loadConfig) {
        this.businessType = businessType;
        this.loadConfig = loadConfig;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void load(ETLExtractor extractor,
                                  ETLTransformer transformer) {

        try {
            Double flag = Math.random();
            logger.info("@@@ [FLAG: {}] 准备获取 pstmtObject 对象 ...", flag);
            // 静态 PreparedStatement 对象获取
            Map<String, Object> pstmtObject = PreparedStatementPool.getPreparedStatement(businessType, loadConfig);
            logger.info("@@@ [FLAG: {}] 已获取 pstmtObject 对象 [HASHCODE: {}]", flag, pstmtObject.hashCode());
            // 获取连接
            Connection conn = (Connection) pstmtObject.get("conn");
            // 获取 PrepareStatement
            PreparedStatement pstmt = (PreparedStatement) pstmtObject.get("pstmt");
            // 获取参数
            Map<Integer, String> params = (Map<Integer, String>) pstmtObject.get("params");
            // 获取数据类型
            Map<String, Integer> types = (Map<String, Integer>) pstmtObject.get("types");
            // 获取遍历器
            long lcnt = 0; // 处理记录数

            Iterator<Map<String, Object>> iter = extractor.walker();

            Map<String, Object> data = null;

            while (iter.hasNext()) {
                data = iter.next();
                transformer.transform(data);

//				logger.debug("@@@ 待导入数据 {}", data);

                // *************************************************
                // logger.info("处理行号：{}", row + 1);
                // *************************************************
                lcnt++;
                DatabaseHelper.fillParamters(pstmt, data, params, types);
                pstmt.addBatch();
                if (lcnt % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    conn.commit(); // 提交
                    pstmt.clearBatch();
                    logger.info("已处理：" + lcnt + " 条");
                }
            }

            pstmt.executeBatch();
            conn.commit(); // 提交
            logger.info("已处理：" + lcnt + " 条");
            logger.info("共计：" + lcnt + " 条");

        } catch (SQLException e) {
            logger.error("处理异常：" + e.getMessage());
        }
    }

}
