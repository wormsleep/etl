package zw.wormsleep.tools.etl.loader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.ETLLoader;
import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.LoadConfig;

public class TextLoader implements ETLLoader {
	final Logger logger = LoggerFactory.getLogger(TextLoader.class);

	private final int BUFFER_SIZE = 20 * 1024 * 1024;

	private LoadConfig loadConfig;
	private File out;
	private String encoding;
	private String seperator;

	public TextLoader(LoadConfig loadConfig, File out) {
		this.loadConfig = loadConfig;
		this.out = out;
		init();
	}

	private void init() {
		this.encoding = loadConfig.getEncoding();
		this.seperator = loadConfig.getSeparator();
	}

	@Override
	public void load(ETLExtractor extractor, ETLTransformer transformer) {
		BufferedWriter writer = null;
		
		try {
			writer = new BufferedWriter(new FileWriterWithEncoding(out, encoding), BUFFER_SIZE);

			// 准备处理数据抽取的数据
			Iterator<Map<String, Object>> iter = extractor.walker();

			// 单行抽取数据
			Map<String, Boolean> fields = loadConfig.getFields();
			Map<String, Object> row = null;
			List<Object> line = new ArrayList<Object>();
			while (iter.hasNext()) {
				row = iter.next();

				for (String field : fields.keySet()) {
					line.add(row.get(field));
				}
				writer.newLine();
				writer.write(StringUtils.join(line.iterator(), seperator));
				line.clear();
			}
		} catch (IOException e) {
			logger.error("IO 异常 !", e);
		} finally {
			if(writer != null) {
				try {
					writer.flush();
					writer.close();
					writer = null;
				} catch (IOException e) {
					logger.error("IO 异常 !", e);
				}
			}
		}

	}
}
