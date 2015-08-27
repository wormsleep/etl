package zw.wormsleep.tools.etl.extractor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.config.ExtractConfig;
import zw.wormsleep.tools.etl.utils.ExcelUtils;

public class ExcelExtractor implements ETLExtractor {
	final Logger logger = LoggerFactory.getLogger(ExcelExtractor.class);

	private Sheet sheet;
	private ExtractConfig extractConfig;
	private int rowStart = 0;
	private int rowEnd = 0;
	private Map<String, Integer> columnPosition = new HashMap<String, Integer>();

	public ExcelExtractor(Sheet sheet, ExtractConfig extractConfig)
			throws InvalidFormatException, IOException {
		this.sheet = sheet;
		this.extractConfig = extractConfig;
		initial();
	}

	@SuppressWarnings("unchecked")
	private void initial() {

		int rowCount = sheet.getLastRowNum();

		boolean hasHeader = extractConfig.hasHeader();
		int definedRowStart = extractConfig.getRowStart();
		int definedRowEnd = extractConfig.getRowEnd();
		int definedMaxHeaderCheckRows = extractConfig.getMaxHeaderCheckRows();

		

		// 若需要进行匹配检查，则进行匹配组装
		if (hasHeader) {
			Map<String, String> checkColumns = extractConfig.getCheckColumns();
			
			Map<String, Object> matchResult = ExcelUtils.getMatchedInfo(
					sheet, checkColumns, definedMaxHeaderCheckRows);

			if (matchResult.get("matched").equals(true)) {
				rowStart = (Integer) matchResult.get("rowstart");
				if (definedRowEnd > 0) {
					rowEnd = Math.min(definedRowEnd, rowCount);
				} else {
					rowEnd = rowCount;
				}
				columnPosition = (Map<String, Integer>) matchResult.get("position");
			}

		} else { // 进行非匹配组装

			rowStart = Math.max(0, definedRowStart);
			rowEnd = Math.max(rowCount, definedRowStart);
			
			columnPosition = (Map<String, Integer>) extractConfig.getIndexedColumns();

		}

		logger.debug("@@@ - 数据抽取 - 起始行 {} 终止行 {} 定位列 {}", rowStart, rowEnd,
				columnPosition);

	}

	@Override
	public Iterator<Map<String, Object>> walker() {
		return new Walker();
	}

	private class Walker implements Iterator<Map<String, Object>> {
		private int rowIndex = rowStart;

		@Override
		public Map<String, Object> next() {
			Map<String, Object> map = new HashMap<String, Object>();

			Row row = sheet.getRow(rowIndex);

			for (String field : columnPosition.keySet()) {
				Cell cell = row.getCell(columnPosition.get(field));
				map.put(field, ExcelUtils.getCellValue(cell));
			}

			rowIndex++;

			return map;
		}

		@Override
		public boolean hasNext() {
			return (rowIndex <= rowEnd);
		}

		@Override
		public void remove() {

		}

	}

}
