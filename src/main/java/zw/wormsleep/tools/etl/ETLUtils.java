package zw.wormsleep.tools.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;

import zw.wormsleep.tools.etl.config.ExtractConfig;
import zw.wormsleep.tools.etl.config.LoadConfig;
import zw.wormsleep.tools.etl.config.SimpleExtractConfig;
import zw.wormsleep.tools.etl.config.SimpleLoadConfig;
import zw.wormsleep.tools.etl.config.SimpleTransformConfig;
import zw.wormsleep.tools.etl.config.TransformConfig;
import zw.wormsleep.tools.etl.database.DatabaseHelper;
import zw.wormsleep.tools.etl.extractor.DatabaseExtractor;
import zw.wormsleep.tools.etl.extractor.ExcelExtractor;
import zw.wormsleep.tools.etl.extractor.TextETLExtractor;
import zw.wormsleep.tools.etl.extractor.XmlExtractor;
import zw.wormsleep.tools.etl.loader.DatabaseLoader;
import zw.wormsleep.tools.etl.loader.DatabasePlusLoader;
import zw.wormsleep.tools.etl.loader.GExcelLoader;
import zw.wormsleep.tools.etl.loader.TextLoader;
import zw.wormsleep.tools.etl.transformer.SimpleETLTransformer;

public class ETLUtils {

	/**
	 * 数据库对数据库
	 * 
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 */
	public static void database2database(String businessType)
			throws ConfigurationException {
		database2database(businessType, null, null);

	}

	/**
	 * 数据库对数据库 - 自定义转换规则
	 * 
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 */
	public static void database2database(String businessType,
			ETLTransformer transformer) throws ConfigurationException {
		database2database(businessType, null, transformer);
	}

	/**
	 * 数据库对数据库 - 自定义转换规则（SQL带参数）
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @throws ConfigurationException
	 */
	public static void database2database(String businessType,
			Map<String, String> parameters) throws ConfigurationException {
		database2database(businessType, parameters, null);
	}

	/**
	 * 数据库对数据库 - 自定义转换规则（SQL带参数）
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 */
	public static void database2database(String businessType,
			Map<String, String> parameters, ETLTransformer transformer)
			throws ConfigurationException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (parameters != null) {
			extractor = new DatabaseExtractor(extractConfig, parameters);
		} else {
			extractor = new DatabaseExtractor(extractConfig);
		}

		if (transformer == null) {
			transformer = new SimpleETLTransformer(new SimpleTransformConfig(
					businessType));
		}

		ETLLoader loader = new DatabaseLoader(loadConfig);

		loader.load(extractor, transformer);
	}

	/**
	 * 数据库对数据库 - 自定义转换规则（SQL带参数）
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param transformer
	 *            转换规则
	 * @param configuration
	 *            配置文件
	 * @throws ConfigurationException
	 */
	public static void database2database(String businessType,
			Map<String, String> parameters, ETLTransformer transformer,
			File configuration) throws ConfigurationException {

		ExtractConfig extractConfig = null;
		LoadConfig loadConfig = null;

		if (configuration != null && configuration.canRead()) {
			extractConfig = new SimpleExtractConfig(businessType, configuration);
			loadConfig = new SimpleLoadConfig(businessType, configuration);
		} else {
			extractConfig = new SimpleExtractConfig(businessType);
			loadConfig = new SimpleLoadConfig(businessType);
		}

		ETLExtractor extractor = null;
		if (parameters != null) {
			extractor = new DatabaseExtractor(extractConfig, parameters);
		} else {
			extractor = new DatabaseExtractor(extractConfig);
		}

		if (transformer == null && configuration.canRead()) {
			transformer = new SimpleETLTransformer(new SimpleTransformConfig(
					businessType));
		}

		ETLLoader loader = new DatabaseLoader(loadConfig);

		loader.load(extractor, transformer);
	}

	/**
	 * 同构或异构数据库表对表拷贝
	 * 
	 * @param businessType
	 * @throws ConfigurationException
	 */
	public static void tables2tables(String businessType)
			throws ConfigurationException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		String srcDatabase = extractConfig.getDatabase();
		String srcCatalog = extractConfig.getCatalog();
		String srcSchemaPattern = extractConfig.getSchemaPattern();
		String srcTableNamePattern = extractConfig.getTablePattern();

		String destDatabase = loadConfig.getDatabase();
		String destCatalog = loadConfig.getCatalog();
		String destSchemaPattern = loadConfig.getSchemaPattern();
		boolean dropandcreate = loadConfig.getAutoCreateTable();
		boolean copydata = loadConfig.getTransmitData();
		int threadCount = loadConfig.getThreadCount();

		DatabaseHelper.tables2tables(srcDatabase, srcCatalog, srcSchemaPattern,
				srcTableNamePattern, destDatabase, destCatalog,
				destSchemaPattern, dropandcreate, copydata, threadCount);
	}

	/**
	 * Excel 文件(流)对数据库
	 * 
	 * @param inp
	 *            输入流
	 * @param businessType
	 *            业务类型
	 * @throws InvalidFormatException
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	public static void excel2database(InputStream inp, String businessType)
			throws InvalidFormatException, IOException, ConfigurationException {

		Workbook wb = WorkbookFactory.create(inp);

		excel2database(wb, businessType, null);

		wb.close();

	}

	/**
	 * Excel 文件(流)对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws InvalidFormatException
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	public static void excel2database(File in, String businessType)
			throws InvalidFormatException, IOException, ConfigurationException {

		Workbook wb = WorkbookFactory.create(in);

		excel2database(wb, businessType, null);

		wb.close();

	}

	/**
	 * Excel 文件(流)对数据库
	 * 
	 * @param inp
	 *            输入流
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws InvalidFormatException
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	public static void excel2database(InputStream inp, String businessType,
			ETLTransformer transformer) throws InvalidFormatException,
			IOException, ConfigurationException {

		Workbook wb = WorkbookFactory.create(inp);

		excel2database(wb, businessType, transformer);

		wb.close();

	}

	/**
	 * Excel 文件(流)对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws InvalidFormatException
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	public static void excel2database(File in, String businessType,
			ETLTransformer transformer) throws InvalidFormatException,
			IOException, ConfigurationException {

		Workbook wb = WorkbookFactory.create(in);

		excel2database(wb, businessType, transformer);

		wb.close();

	}

	/**
	 * Excel 文件 ETL 处理方法
	 * 
	 * @param wb
	 *            POI Workbook 对象
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws InvalidFormatException
	 * @throws IOException
	 */
	public static void excel2database(Workbook wb, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			InvalidFormatException, IOException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		int sheetCount = wb.getNumberOfSheets();
		Sheet sheet = null;
		for (int i = 0; i < sheetCount; i++) {
			sheet = wb.getSheetAt(i);
			ETLExtractor extractor = new ExcelExtractor(sheet, extractConfig);
			if (transformer == null) {
				transformer = new SimpleETLTransformer(
						new SimpleTransformConfig(businessType));
			}
			ETLLoader loader = new DatabaseLoader(loadConfig);

			loader.load(extractor, transformer);
		}
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2database(File in, String businessType)
			throws ConfigurationException, FileNotFoundException {
		text2databaseProcessor(in, businessType, null);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2database(InputStream in, String businessType)
			throws ConfigurationException, FileNotFoundException {
		text2databaseProcessor(in, businessType, null);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2database(File in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException {
		text2databaseProcessor(in, businessType, transformer);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2database(InputStream in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException {
		text2databaseProcessor(in, businessType, transformer);
	}

	/**
	 * 数据文件 ETL 处理方法
	 * 
	 * @param in
	 * @param businessType
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	private static void text2databaseProcessor(Object in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (in instanceof File) {
			extractor = new TextETLExtractor((File) in, extractConfig);
		} else if (in instanceof InputStream) {
			extractor = new TextETLExtractor((InputStream) in, extractConfig);
		}

		if (transformer == null) {
			transformer = new SimpleETLTransformer(new SimpleTransformConfig(
					businessType));
		}

		ETLLoader loader = new DatabaseLoader(loadConfig);

		loader.load(extractor, transformer);

	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2databasePlus(File in, String businessType)
			throws ConfigurationException, FileNotFoundException {
		text2databasePlusProcessor(in, businessType, null);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2databasePlus(InputStream in, String businessType)
			throws ConfigurationException, FileNotFoundException {
		text2databasePlusProcessor(in, businessType, null);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2databasePlus(File in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException {
		text2databasePlusProcessor(in, businessType, transformer);
	}

	/**
	 * 文本(数据)文件对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void text2databasePlus(InputStream in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException {
		text2databasePlusProcessor(in, businessType, transformer);
	}

	/**
	 * 数据文件 ETL 处理方法
	 * 
	 * @param in
	 * @param businessType
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 */
	private static void text2databasePlusProcessor(Object in,
			String businessType, ETLTransformer transformer)
			throws ConfigurationException, FileNotFoundException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (in instanceof File) {
			extractor = new TextETLExtractor((File) in, extractConfig);
		} else if (in instanceof InputStream) {
			extractor = new TextETLExtractor((InputStream) in, extractConfig);
		}

		if (transformer == null) {
			transformer = new SimpleETLTransformer(new SimpleTransformConfig(
					businessType));
		}

		ETLLoader loader = new DatabasePlusLoader(businessType, loadConfig);

		loader.load(extractor, transformer);

	}

	/**
	 * XML对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(File in, String businessType)
			throws ConfigurationException, FileNotFoundException,
			DocumentException {
		xml2databaseProcessor(in, businessType, null);
	}

	/**
	 * XML对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(InputStream in, String businessType)
			throws ConfigurationException, FileNotFoundException,
			DocumentException {
		xml2databaseProcessor(in, businessType, null);
	}

	/**
	 * XML对数据库
	 * 
	 * @param document
	 *            dom4j document
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(Document document, String businessType)
			throws ConfigurationException, FileNotFoundException,
			DocumentException {
		xml2databaseProcessor(document, businessType, null);
	}

	/**
	 * XML对数据库
	 * 
	 * @param text
	 *            XML 字符串
	 * @param businessType
	 *            业务类型
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(String text, String businessType)
			throws ConfigurationException, FileNotFoundException,
			DocumentException {
		xml2databaseProcessor(text, businessType, null);
	}

	/**
	 * XML对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(File in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException, DocumentException {
		xml2databaseProcessor(in, businessType, transformer);
	}

	/**
	 * XML对数据库
	 * 
	 * @param in
	 *            文件
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(InputStream in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException, DocumentException {
		xml2databaseProcessor(in, businessType, transformer);
	}

	/**
	 * XML对数据库
	 * 
	 * @param document
	 *            dom4j document
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(Document document, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException, DocumentException {
		xml2databaseProcessor(document, businessType, transformer);
	}

	/**
	 * XML对数据库
	 * 
	 * @param text
	 *            XML 字符串
	 * @param businessType
	 *            业务类型
	 * @param transformer
	 *            转换规则
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	public static void xml2database(String text, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException, DocumentException {
		xml2databaseProcessor(text, businessType, transformer);
	}

	/**
	 * XML数据文件 ETL 处理方法
	 * 
	 * @param in
	 * @param businessType
	 * @param transformer
	 * @throws ConfigurationException
	 * @throws FileNotFoundException
	 * @throws DocumentException
	 */
	private static void xml2databaseProcessor(Object in, String businessType,
			ETLTransformer transformer) throws ConfigurationException,
			FileNotFoundException, DocumentException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);
		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (in instanceof File) {
			extractor = new XmlExtractor((File) in, extractConfig);
		} else if (in instanceof InputStream) {
			extractor = new XmlExtractor((InputStream) in, extractConfig);
		} else if (in instanceof String) {
			extractor = new XmlExtractor((String) in, extractConfig);
		} else if (in instanceof Document) {
			extractor = new XmlExtractor((Document) in, extractConfig);
		}

		if (transformer == null) {
			transformer = new SimpleETLTransformer(new SimpleTransformConfig(
					businessType));
		}

		ETLLoader loader = new DatabaseLoader(loadConfig);

		loader.load(extractor, transformer);

	}

	/**
	 * 数据库对 Excel
	 * 
	 * @param businessType
	 *            业务类型
	 * @param template
	 *            Excel 模板文件
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2GExcel(String businessType, File template,
			File destination) throws ConfigurationException {
		database2GExcel(businessType, null, null, template, null, destination);
	}

	/**
	 * 数据库对 Excel
	 * 
	 * @param businessType
	 *            业务类型
	 * @param template
	 *            Excel 模板文件
	 * @param temParams
	 *            目标文件其他参数（例如：标题、日期、单位等）
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2GExcel(String businessType, File template,
			Map<String, Object> temParams, File destination)
			throws ConfigurationException {
		database2GExcel(businessType, null, null, template, temParams,
				destination);
	}

	/**
	 * 数据库对 Excel
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param template
	 *            Excel 模板文件
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2GExcel(String businessType,
			Map<String, String> parameters, File template, File destination)
			throws ConfigurationException {
		database2GExcel(businessType, parameters, null, template, null,
				destination);
	}

	/**
	 * 数据库对 Excel
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param template
	 *            Excel 模板文件
	 * @param temParams
	 *            目标文件其他参数（例如：标题、日期、单位等）
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2GExcel(String businessType,
			Map<String, String> parameters, File template,
			Map<String, Object> temParams, File destination)
			throws ConfigurationException {
		database2GExcel(businessType, parameters, null, template, temParams,
				destination);
	}

	/**
	 * 数据库对 Excel （GExcel 模板文件）
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param transformer
	 *            转换规则
	 * @param template
	 *            模板文件
	 * @param temParams
	 *            目标文件其他参数（例如：标题、日期、单位等）
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2GExcel(String businessType,
			Map<String, String> parameters, ETLTransformer transformer,
			File template, Map<String, Object> temParams, File destination)
			throws ConfigurationException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);

		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (parameters != null) {
			extractor = new DatabaseExtractor(extractConfig, parameters);
		} else {
			extractor = new DatabaseExtractor(extractConfig);
		}

		if (transformer == null) {
			TransformConfig transformConfig = new SimpleTransformConfig(
					businessType);
			transformer = new SimpleETLTransformer(transformConfig);
		}

		ETLLoader loader = null;
		if (temParams != null) {
			loader = new GExcelLoader(loadConfig, template, temParams,
					destination);
		} else {
			loader = new GExcelLoader(loadConfig, template, destination);
		}

		loader.load(extractor, transformer);

	}

	/**
	 * 数据库对 Text
	 * 
	 * @param businessType
	 *            业务类型
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2Text(String businessType, File destination)
			throws ConfigurationException {
		database2Text(businessType, null, null, destination);
	}

	/**
	 * 数据库对 Text
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2Text(String businessType,
			Map<String, String> parameters, File destination)
			throws ConfigurationException {
		database2Text(businessType, parameters, null, destination);
	}

	/**
	 * 数据库对 Text
	 * 
	 * @param businessType
	 *            业务类型
	 * @param parameters
	 *            替换 input->sql 中的参数值
	 * @param transformer
	 *            转换规则
	 * @param destination
	 *            目标文件
	 * @throws ConfigurationException
	 */
	public static void database2Text(String businessType,
			Map<String, String> parameters, ETLTransformer transformer,
			File destination) throws ConfigurationException {
		ExtractConfig extractConfig = new SimpleExtractConfig(businessType);

		LoadConfig loadConfig = new SimpleLoadConfig(businessType);

		ETLExtractor extractor = null;
		if (parameters != null) {
			extractor = new DatabaseExtractor(extractConfig, parameters);
		} else {
			extractor = new DatabaseExtractor(extractConfig);
		}

		if (transformer == null) {
			TransformConfig transformConfig = new SimpleTransformConfig(
					businessType);
			transformer = new SimpleETLTransformer(transformConfig);
		}

		ETLLoader loader = new TextLoader(loadConfig, destination);

		loader.load(extractor, transformer);

	}

	/**
	 * 辅助类 - 处理格式化的日期
	 * 
	 * @author zhaowei
	 * 
	 */
	public static class DateFormatHelper {

		public static final String YYYY_MM_DD = "yyyy-MM-dd";
		public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
		public static final String YYYYMM = "yyyyMM";
		public static final String[] PARSEPATTERNS = new String[] { "yyyy-MM",
				"yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
				"yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss" };

		/**
		 * 年月遍历器
		 * 
		 * @param start
		 *            起始年月
		 * @param end
		 *            终止年月
		 * @return
		 * @throws ParseException
		 */
		public static Iterator<String> monthIterator(final String start,
				final String end) throws ParseException {

			return new Iterator<String>() {
				private Calendar calStart = DateUtils.toCalendar(DateUtils
						.parseDate(start, PARSEPATTERNS));
				private Calendar calEnd = DateUtils.toCalendar(DateUtils
						.parseDate(end, PARSEPATTERNS));

				@Override
				public boolean hasNext() {
					return (getYYYYMM(calStart).compareTo(getYYYYMM(calEnd)) <= 0 ? true
							: false);
				}

				@Override
				public String next() {
					String result = getYYYYMM(calStart);
					calStart.add(Calendar.MONTH, 1);
					return result;
				}

				@Override
				public void remove() {
				}
			};

		}

		public static String getYearFirstDay() {
			return getYearFirstDay(Calendar.getInstance());
		}

		public static String getYearFirstDay(String str) throws ParseException {
			return getYearFirstDay(DateUtils.toCalendar(DateUtils.parseDate(
					str, PARSEPATTERNS)));
		}

		public static String getYearFirstDay(Calendar calendar) {
			return getYearFirstDay(calendar, YYYY_MM_DD);
		}

		public static String getYearFirstDay(Calendar calendar, String pattern) {
			calendar.set(Calendar.MONTH, 0);
			calendar.set(Calendar.DATE, 1);

			return format(calendar, pattern);
		}

		public static String getMonthFirstDay() {
			return getMonthFirstDay(Calendar.getInstance());
		}

		public static String getMonthFirstDay(String str) throws ParseException {
			return getMonthFirstDay(DateUtils.toCalendar(DateUtils.parseDate(
					str, PARSEPATTERNS)));
		}

		public static String getMonthFirstDay(Calendar calendar) {
			return getMonthFirstDay(calendar, YYYY_MM_DD_HH_MM_SS);
		}

		public static String getMonthFirstDay(Calendar calendar, String pattern) {
			calendar.set(Calendar.DATE, 1);

			return format(calendar, pattern);
		}

		public static String getMonthLastDay() {
			return getMonthLastDay(Calendar.getInstance());
		}

		public static String getMonthLastDay(String str) throws ParseException {
			return getMonthLastDay(DateUtils.toCalendar(DateUtils.parseDate(
					str, PARSEPATTERNS)));
		}

		public static String getMonthLastDay(Calendar calendar) {
			return getMonthLastDay(calendar, YYYY_MM_DD_HH_MM_SS);
		}

		public static String getMonthLastDay(Calendar calendar, String pattern) {
			calendar.set(Calendar.DATE, 1);
			calendar.add(Calendar.MONTH, 1);
			calendar.add(Calendar.DATE, -1);
			calendar.set(Calendar.HOUR, 23);
			calendar.set(Calendar.MINUTE, 59);
			calendar.set(Calendar.SECOND, 59);

			return format(calendar, pattern);
		}

		public static String getYYYYMM() {
			Calendar calendar = Calendar.getInstance();
			return getYYYYMM(calendar);
		}

		public static String getYYYYMM(String str) throws ParseException {
			Calendar calendar = DateUtils.toCalendar(DateUtils.parseDate(str,
					PARSEPATTERNS));
			return getYYYYMM(calendar);
		}

		public static String getYYYYMM(Calendar calendar) {
			return format(calendar, YYYYMM);
		}

		public static String getYYYYMMHHMMSS() {
			Calendar calendar = Calendar.getInstance();
			return getYYYYMMHHMMSS(calendar);
		}

		public static String getYYYYMMHHMMSS(String str) throws ParseException {
			Calendar calendar = DateUtils.toCalendar(DateUtils.parseDate(str,
					PARSEPATTERNS));
			return getYYYYMMHHMMSS(calendar);
		}

		public static String getYYYYMMHHMMSS(Calendar calendar) {
			return format(calendar, YYYY_MM_DD_HH_MM_SS);
		}

		public static String format(Calendar calendar, String pattern) {
			return DateFormatUtils.format(calendar, pattern);
		}
	}

	/**
	 * 类辅助 ( 暂不使用 )
	 * 
	 * 功能: 自动进行 CLASS 加载
	 * 
	 * @author zhaowei
	 * 
	 */
	@SuppressWarnings("unused")
	private static class ClassHelper {
		private static String DEFAULT_EXTRACTOR_PACKAGE = "zw.wormsleep.tools.etl.extractor.";
		private static String DEFAULT_LOADER_PACKAGE = "zw.wormsleep.tools.etl.loader.";
		private static String DEFAULT_INPUT_TYPE = "file";
		private static String DEFAULT_OUTPUT_TYPE = "database";

		static ETLExtractor getETLExtractorInstance(
				ExtractConfig extractConfig, String name, Object in,
				Map<String, String> parameters) throws ClassNotFoundException {
			ETLExtractor extractor = null;
			if (name.equalsIgnoreCase(DEFAULT_INPUT_TYPE)) {
				// extractor = new SimpleETLExtractor(in, extractConfig);
			} else {
				// Class _extractor = Class.forName(DEFAULT_EXTRACTOR_PACKAGE
				// + toUpperFirstLetter(name) + "Extractor");
				// Constructor c = _extractor.getConstructor(arg0)
			}

			return extractor;
		}

		static String toUpperFirstLetter(String name) {
			String firstLetter = name.substring(0, 1);
			String remaining = name.substring(1);

			return firstLetter.toUpperCase() + remaining;
		}
	}

}
