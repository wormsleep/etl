package zw.wormsleep.tools.etl.multitask.task;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.ETLLoader;
import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.ExtractConfig;
import zw.wormsleep.tools.etl.config.LoadConfig;
import zw.wormsleep.tools.etl.config.SimpleExtractConfig;
import zw.wormsleep.tools.etl.config.SimpleLoadConfig;
import zw.wormsleep.tools.etl.config.SimpleTransformConfig;
import zw.wormsleep.tools.etl.config.TransformConfig;
import zw.wormsleep.tools.etl.extractor.DatabaseExtractor;
import zw.wormsleep.tools.etl.loader.DatabaseLoader;
import zw.wormsleep.tools.etl.multitask.Task;
import zw.wormsleep.tools.etl.transformer.SimpleETLTransformer;

public class Table2TableTask implements Task {
	final Logger logger = LoggerFactory.getLogger(Table2TableTask.class);
	
	private String businessType;
	private File configuration;

	public Table2TableTask(String businessType, File configuration) {
		this.businessType = businessType;
		this.configuration = configuration;
	}

	public void execute() {
		try {
			ExtractConfig extractConfig = new SimpleExtractConfig(businessType, configuration);
			TransformConfig transformConfig = new SimpleTransformConfig(businessType, configuration);
			LoadConfig loadConfig = new SimpleLoadConfig(businessType, configuration);

			ETLExtractor extractor = new DatabaseExtractor(extractConfig);
			ETLTransformer transformer = new SimpleETLTransformer(transformConfig);
			ETLLoader loader = new DatabaseLoader(loadConfig);

			loader.load(extractor, transformer);
		}catch (ConfigurationException e) {
			logger.error("配置异常 !", e);
		}
	}
}
