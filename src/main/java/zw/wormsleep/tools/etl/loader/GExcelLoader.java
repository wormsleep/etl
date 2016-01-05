package zw.wormsleep.tools.etl.loader;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gisinfo.common.excel.ExcelWriteException;
import com.gisinfo.common.excel.ExcelWriter;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.ETLLoader;
import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.LoadConfig;

public class GExcelLoader implements ETLLoader {
	final Logger logger = LoggerFactory.getLogger(GExcelLoader.class);

	private int MAX_SHEET_ROWS = 60000;
	private String TEMPLATE_COLLECTION = "data1";

	private LoadConfig loadConfig;
	private File template;
	private File destination;
	private Map<String, Object> parameters = new HashMap<String, Object>();

	private int maxSheetRows = 0;
	private String templateCollection;

	public GExcelLoader(LoadConfig loadConfig, File template, File destination) {
		this.loadConfig = loadConfig;
		this.template = template;
		this.destination = destination;

		initial();
	}

	public GExcelLoader(LoadConfig loadConfig, File template,
			Map<String, Object> parameters, File destination) {
		this.loadConfig = loadConfig;
		this.template = template;
		this.parameters = parameters;
		this.destination = destination;

		initial();
	}

	private void initial() {
		int maxrowsperfile = loadConfig.getMaxRowsNumberPerFile();
		maxSheetRows = maxrowsperfile > 0 ? maxrowsperfile : MAX_SHEET_ROWS;
		String templatecollection = loadConfig.getTemplateCollection();
		templateCollection = (templatecollection != null && !templatecollection.equals("")) ? templatecollection : TEMPLATE_COLLECTION;
		
	}

	@Override
	public void load(ETLExtractor extractor, ETLTransformer transformer) {
		// 目标文件基础信息获取
		String parentpath = destination.getParent();
		String filename = destination.getName();
		String filebasename = FilenameUtils.getBaseName(filename);
		String extension = FilenameUtils.getExtension(filename);
		
		ExcelWriter writer = new ExcelWriter();
		// 准备模板数据集 Map
		Map<String, Object> values = new HashMap<String, Object>();
		// 将参数加入（如：标题等）
		values.putAll(parameters);
		// 准备模板数据 data
		List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
		// 准备处理数据抽取的数据
		Iterator<Map<String, Object>> iter = extractor.walker();
		// 处理行数记数
		int index = 0;
		// 文件名递增记数（若已处理行数大于单个文件已定义最大行数，则新建子文件，其文件名递增 1）
		int filenameindex = 0;
		// 单行抽取数据
		Map<String, Object> row = null;
		while (iter.hasNext()) {
			row = iter.next();
			// 优化数据正确性
			if(row.size() < 1) {
				continue;
			}
			// 转换抽取数据
			transformer.transform(row);
			data.add(row);
			index++;
			// 若已处理行数大于单个文件已定义最大行数，则新建子文件，其文件名递增 1
			if (index >= maxSheetRows) {
				try {
					// 子文件名生成
					String subfile = parentpath + "/" + filebasename
							+ String.valueOf(filenameindex) + "." + extension;
					// 子文件序号递增 1
					filenameindex++;
					// 子文件生成
					File subDestination = new File(subfile);
					logger.debug("@@@ 子文件：{}", subDestination.getPath());
					// 装载数据集
					values.put(templateCollection, data);
					// 写入文件
					writer.writeExcel(values, template, subDestination);
					logger.info("@@@ 输出子文件: {}", subDestination.getAbsolutePath());
					// 重置计数及数据集对象
					index = 0;
					values.remove(templateCollection);
					data = new ArrayList<Map<String, Object>>(); 
					
				} catch (ExcelWriteException e) {
					logger.error("@@@ GExcel 写操作异常 !", e);
				}

			}
		}
		// 装载（剩余）数据集
		values.put(templateCollection, data);
		
		// 若已进行文件拆分，则按子文件处理
		if(filenameindex > 0) {
			try {
				String subfile = parentpath + "/" + filebasename
						+ String.valueOf(filenameindex) + "." + extension;
				File subDestination = new File(subfile);
				writer.writeExcel(values, template, subDestination);
				logger.info("@@@ 输出子文件: {}", subDestination.getAbsolutePath());
			} catch (ExcelWriteException e) {
				logger.error("@@@ GExcel 写操作异常 !", e);
			}
		} else { // 若未进行文件拆分，则按默认处理
			try {
				if(data.size() > 0) {
					writer.writeExcel(values, template, destination);
					logger.info("@@@ 输出文件: {}", destination.getAbsolutePath());
				}
			} catch (ExcelWriteException e) {
				logger.error("@@@ GExcel 写操作异常 !", e);
			}
		}
		
	}
}
