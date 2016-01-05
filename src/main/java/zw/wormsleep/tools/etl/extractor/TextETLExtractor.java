package zw.wormsleep.tools.etl.extractor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.config.ExtractConfig;

public class TextETLExtractor implements ETLExtractor {
    final Logger logger = LoggerFactory.getLogger(TextETLExtractor.class);

    private BufferedInputStream inp;

    private final int BUFFER_SIZE = 20 * 1024 * 1024;

    private String encoding = "UTF-8";
    private String separator = "\t";
    private ExtractConfig extractConfig;
    private Map<String, Integer> columnPosition = new HashMap<String, Integer>();
    private int columnCount = -1;

    public TextETLExtractor(File in, ExtractConfig extractConfig)
            throws FileNotFoundException {
        inp = new BufferedInputStream(new FileInputStream(in), BUFFER_SIZE);
        this.extractConfig = extractConfig;
        initial();
    }

    public TextETLExtractor(InputStream ins, ExtractConfig extractConfig) {
        inp = new BufferedInputStream(ins, BUFFER_SIZE);
        this.extractConfig = extractConfig;
        initial();
    }

    private void initial() {
        encoding = extractConfig.getEncoding();
        separator = extractConfig.getSeparator();
        columnPosition = extractConfig.getIndexedColumns();
        columnCount = columnPosition.size();

        logger.debug(
                "@@@ - 抽取初始化 - Encoding {} Seperaotr {} columnPosition {}",
                encoding, separator, columnPosition);
    }

    @Override
    public Iterator<Map<String, Object>> walker() {
        return new Walker();
    }

    private class Walker implements Iterator<Map<String, Object>> {
        private BufferedReader reader;
        private String line = null;

        public Walker() {
            try {
                reader = new BufferedReader(
                        new InputStreamReader(inp, encoding));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        @Override
        public boolean hasNext() {
            boolean result = false;
            try {
                line = reader.readLine();
                if (line != null) {
                    result = true;
                } else {
                    reader.close();
                    reader = null;
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            return result;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> map = new HashMap<String, Object>();

            String[] values = line.split(separator);

            // 优化数据正确性
            if (values.length >= columnCount) {
                for (String key : columnPosition.keySet()) {
                    map.put(key, values[columnPosition.get(key)].trim());
                }
            }

            return map;
        }

        @Override
        public void remove() {
        }

    }

}
