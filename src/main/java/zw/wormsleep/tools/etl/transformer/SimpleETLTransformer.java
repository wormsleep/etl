package zw.wormsleep.tools.etl.transformer;

import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.TransformConfig;

import java.util.Map;

public class SimpleETLTransformer implements ETLTransformer {
    private TransformConfig transformConfig;

    public SimpleETLTransformer(TransformConfig transformConfig) {
        this.transformConfig = transformConfig;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void transform(Map<String, Object> row) {
        Map<String, Object> transformColums = transformConfig
                .getTransformColumns();

        for (String key : transformColums.keySet()) {
            Object obj = transformColums.get(key);

            // 对应映射类型的转换处理，其它的直接覆盖 row 中的值
            if (obj instanceof Map) {
                // 1、获取抽取记录中 key 对应的值 rowValue
                Object rowValue = row.get(key);
                // 2、将 rowValue 与映射对象的 key 值进行匹配并取值
                if (rowValue != null) {
                    Object mappingValue = ((Map) obj).get(String
                            .valueOf(rowValue));
                    // 3、若有对应值则转换，若无则原值输出
                    if (mappingValue != null) {
                        row.put(key, mappingValue);
                    }
                }
            } else if (obj instanceof Formatter) {
                row.put(key, ((Formatter) obj).format(row.get(key)));
            } else {
                row.put(key, obj);
            }
        }
    }

}
