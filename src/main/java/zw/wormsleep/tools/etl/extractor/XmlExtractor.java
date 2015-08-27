package zw.wormsleep.tools.etl.extractor;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.config.ExtractConfig;

public class XmlExtractor implements ETLExtractor {
	final Logger logger = LoggerFactory.getLogger(XmlExtractor.class);

	private ExtractConfig extractConfig;
	private List<Element> elements = new ArrayList<Element>();
	private Map<String, String> checkColumns;

	public XmlExtractor(File in, ExtractConfig extractConfig) throws DocumentException {
		this.extractConfig = extractConfig;
		
		SAXReader reader = new SAXReader();
		reader.setEncoding(extractConfig.getEncoding());
		Document document = reader.read(in);
		
		initial(document);
	}
	
	public XmlExtractor(InputStream ins, ExtractConfig extractConfig) throws DocumentException {
		this.extractConfig = extractConfig;
		
		SAXReader reader = new SAXReader();
		reader.setEncoding(extractConfig.getEncoding());
		Document document = reader.read(ins);
		
		initial(document);
	}
	
	public XmlExtractor(String text, ExtractConfig extractConfig) throws DocumentException {
		this.extractConfig = extractConfig;
		
		Document document = DocumentHelper.parseText(text);
		
		initial(document);
	}
	
	public XmlExtractor(Document document, ExtractConfig extractConfig) throws DocumentException {
		this.extractConfig = extractConfig;
		
		initial(document);
	}
	
	@SuppressWarnings("unchecked")
	private void initial(Document document) {
		String xpath = extractConfig.getXpath();
		
		elements = (List<Element>)document.selectNodes(xpath);
		
		checkColumns = extractConfig.getCheckColumns();
		
	}
	
	

	@Override
	public Iterator<Map<String, Object>> walker() {
		return new Walker();
	}
	
	private class Walker implements Iterator<Map<String, Object>> {
		private int index = 0;

		@Override
		public boolean hasNext() {
			return ((index < elements.size()) ? true : false);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Map<String, Object> next() {
			Map<String, Object> map = new HashMap<String, Object>();
			
			Element element = null;
			for(Iterator iter = elements.get(index).elementIterator(); iter.hasNext();) {
				element = (Element)iter.next();
				String name = element.getName();
				String value = element.getTextTrim();
				
				String columnName = checkColumns.get(name);
				
				map.put(columnName, value);
			}
			
			index++;
			
			return map; 
		}

		@Override
		public void remove() {
		}
		
	}

}
