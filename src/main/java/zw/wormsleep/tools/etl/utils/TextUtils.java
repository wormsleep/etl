package zw.wormsleep.tools.etl.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.text.translate.UnicodeEscaper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextUtils {
	static final Logger logger = LoggerFactory.getLogger(TextUtils.class);
	
	private static final int BUFFER_SIZE = 20 * 1024 * 1024;
	
	/**
	 * 获取分隔符的 Unicode 编码 ( 供源数据为文件类型的使用 )
	 * @param value
	 * @return
	 */
	public static String getSeperatorUnicode(String value) {
		UnicodeEscaper unie = new UnicodeEscaper();
		return unie.translate(value).replace("\\", "\\\\");
	}
	
	/**
	 * 获取文本类数据文件分析数据 ( 供采集时配置文件使用 )
	 * @param in
	 * @param seperator
	 * @param encoding
	 * @param lineCount
	 * @return
	 * @throws IOException 
	 */
	public static List<Map<Integer, Object>> analysisSourceFileStructure(File in, String seperator, String encoding, int lineCount, boolean header, boolean printIt) throws IOException {
		List<Map<Integer, Object>> result = new ArrayList<Map<Integer,Object>>();
		
		BufferedInputStream inp = new BufferedInputStream(new FileInputStream(in), BUFFER_SIZE);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inp, encoding));
		
		int lc = 0;
		Map<Integer, Object> map = null;
		String line = null;
		while((line = reader.readLine()) != null && lc++<lineCount) {
			String[] values = line.split(seperator);
			map = new LinkedHashMap<Integer, Object>();
			for(int i = 0; i<values.length; i++) {
				map.put(i, values[i].trim());
			}
			result.add(map);
		}
		
		reader.close();
		
		if(printIt) {
			System.out.println("-------------- 文本解析后的内容 --------------");
			StringBuffer cfg = new StringBuffer(); 
			int ln = 1;
			for(Map<Integer, Object> m : result) {
				int size = m.size();
				System.out.print("@ 行: "+ ln++ +" 域: "+ size +" @\t");
				for(Integer index : m.keySet()) {
					System.out.print("["+index+"] " + m.get(index) + "\t");
					// 打印域配置
					if(header && ln==2) {
						cfg.append("<column>\n\t<index>"+index+"</index>\n\t<field>"+m.get(index)+"</field>\n</column>\n");
					}
				}
				System.out.println();
			}
			if(header) {
				System.out.println("-------------- 域配置内容 --------------");
				System.out.println(cfg.toString());
			}
		}
		
		return result;
	}
	
	/**
	 * 源文件指定域过滤。
	 * 由于源文件中存在一些域数量不正确 ( 即拆分后的字段数量与期望的不一致 ) 
	 * 导致域错位, 因此先将文件按指定域进行分析, 形成匹配域文件和不匹配域文件
	 * 
	 * @param in 源文件
	 * @param seperator 分隔符
	 * @param encoding 文件编码
	 * @param domainCount 域数量 ( 每行 )
	 * @throws IOException 
	 */
	public static void sourceFileSpecificDomainFilter(File in, String seperator, String encoding, int domainCount) throws IOException {
		BufferedInputStream inp = new BufferedInputStream(new FileInputStream(in), BUFFER_SIZE);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inp, encoding));
		
		// 源文件基本信息
		String filename = in.getName();
		String basename = FilenameUtils.getBaseName(filename);
		String extension = FilenameUtils.getExtension(filename);
		String destPath = in.getParent() != null ? in.getParent() : "";
		// 匹配文件
		String matchFilename = destPath + "/" + basename + "-match" + "." + extension;
		BufferedWriter writerMatch = new BufferedWriter(new FileWriter(new File(matchFilename)), BUFFER_SIZE);
		// 不匹配文件
		String mismatchFilename = destPath + "/" + basename + "-mismatch" + "." + extension;
		BufferedWriter writerMismatch = new BufferedWriter(new FileWriter(new File(mismatchFilename)), BUFFER_SIZE);
		
		String line = null;
		int lineCount = 0;
		int matchLineCount = 0;
		int mismatchLineCount = 0;
		while((line = reader.readLine()) != null) {
			lineCount++;
			String[] values = line.split(seperator);
//			logger.debug("@@@ 行: {} 域: {}", lineCount, values.length);
			if(values.length == domainCount) {
				if(matchLineCount++ > 0)
					writerMatch.newLine();
				writerMatch.write(line);
			} else {
				if(mismatchLineCount++ > 0)
					writerMismatch.newLine();
				writerMismatch.write(line);
			}
		}
		
		logger.info("@@@ 文件: {} [ 域: {} ] 共计 {} 行 -- 分析结果: 匹配域共计 {} 行, 未匹配域共计 {} 行.", filename, domainCount, lineCount, matchLineCount, mismatchLineCount);
		
		writerMatch.flush();
		writerMismatch.flush();
		
		writerMatch.close();
		writerMismatch.close();
		
		writerMatch = null;
		writerMismatch = null;
		
		reader.close();
	}
	
}
