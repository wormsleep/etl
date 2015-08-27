package zw.wormsleep.tools.etl.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelUtils {
	static final Logger logger = LoggerFactory.getLogger(ExcelUtils.class);
	
	/**
	 * 获取 Cell 值（包装为 Object 对象）
	 * 专用于 POI
	 * @param cell
	 * @return
	 */
	public static Object getCellValue(Cell cell) {
		Object value = null;

		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_STRING:
			value = cell.getRichStringCellValue().getString();
			break;
		case Cell.CELL_TYPE_NUMERIC:
			if (DateUtil.isCellDateFormatted(cell)) {
				value = cell.getDateCellValue();
			} else {
				value = cell.getNumericCellValue();
			}
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			value = cell.getBooleanCellValue();
			break;
		case Cell.CELL_TYPE_FORMULA:
			value = cell.getCellFormula();
			break;
		default:
			value = null;
		}
		return value;
	}
	
	/**
	 * 判断并获取匹配的 Header 行号
	 * @param sheet
	 * @param header
	 * @return 
	 */
	public static Map<String, Object> getMatchedInfo(Sheet sheet, Map<String, String> header, int maxHeaderRowIndex) {
		logger.debug("@@@ Excel 列头匹配检查 ... 最大检索行数：{}", maxHeaderRowIndex);
		
		String sheetName = sheet.getSheetName();
		
		// 构造返回集合对象
		Map<String, Object> result = new HashMap<String, Object>();
		
		Map<String, Integer> position = new HashMap<String, Integer>();
		
		boolean matchResult = false;
		
		int rowStart = 0;
		int needSeekCount = header.size();
		int foundCount = 0;
		
		Object sheetColumnName = null;
		
		for(int rowIndex = rowStart; rowIndex < maxHeaderRowIndex; rowIndex++) {
			logger.debug("@@@ 检查第 {} 行", rowIndex);
			Row row = sheet.getRow(rowIndex);
			for(String key : header.keySet()) {
				logger.debug("@@ 查找列名：{}", key);
				boolean foundFirst = false;
				for(Cell cell : row) {
					int columnIndex = cell.getColumnIndex();
					// 判断 head
					sheetColumnName = getCellValue(cell);
					logger.debug("检查列头 - 行 {} 列 {} 值 {}", rowIndex, columnIndex, sheetColumnName);
					if(sheetColumnName != null) {
						if(key.equals(sheetColumnName)) {
							// 定位该列位置
							position.put(header.get(key), columnIndex);
							// 找到对应列个数累加
							foundCount++;
							foundFirst = true;
							logger.debug("@@ 定位列 {} 行号：{} 列号 {} - [ 已定位 {} 个，剩余 {} 个 ]", key, rowIndex, columnIndex,foundCount,(needSeekCount-foundCount));
							break;
						}
					} 
				}
				if(!foundFirst)
					break;
				// 若在该行找到完整的匹配列，定位 sheet 数据采集起始行为 该行+1
				if(foundCount == needSeekCount) {
					rowStart = rowIndex + 1;
					matchResult = true;
					logger.debug("@@@ Sheet {} 已找到匹配列头，数据起始行号：{}", sheetName, rowStart);
					break;
				}
					
			}
			foundCount = 0;
			if(matchResult)
				break;
		}
		
		if(matchResult) {
			result.put("matched", true);
			result.put("rowstart", rowStart);
			result.put("position", position);
		} else {
			result.put("matched", false);
		}
			
		
		return result;
	}
}
