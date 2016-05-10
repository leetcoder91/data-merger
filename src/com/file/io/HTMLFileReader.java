package com.file.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.file.transform.InternalTable;
import com.google.common.base.Charsets;

/**
 * Reads HTML type files and processes them.
 */
public class HTMLFileReader extends AbstractFileReader {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(HTMLFileReader.class);

	// Constructors

	public HTMLFileReader(String idColumnName) {
		super(idColumnName);
	}

	// Operations

	/**
	 * Assuming there is only _one_ top-level (i.e. not nested) table in the _body_ of the HTML
	 * document
	 */
	public InternalTable process(String filename) throws IOException {
		File input = getFile(filename);

		if (input != null) {
			try {
				Document doc = Jsoup.parse(input, Charsets.UTF_8.toString(), "");
				Element body = doc.body();
				Elements table = body.select("table"); // assuming there is a table node
				InternalTable internalTable = null;
				Iterator<Element> rowItr = table.select("tr").iterator(); // assuming there is a tr node

				if (rowItr.hasNext()) {
					Element row = rowItr.next();
					Elements ths = row.select("th"); // assuming headers are in th node

					List<String> colNameList = new ArrayList<String>(ths.size());

					for (Element th : ths) {
						colNameList.add(th.text());
					}

					internalTable = new InternalTable(colNameList, getIdColumnName()); 
				}

				for (; rowItr.hasNext();) {
					Element row = rowItr.next();
					Elements tds = row.select("td"); // assuming data is in td node
					List<String> dataList = new ArrayList<String>(tds.size());

					for (Element td : tds) {
						dataList.add(td.text());
					}

					internalTable.addData(dataList); 
				}

				return internalTable;
			}
			catch (IllegalStateException e) {
				if (LOGGER.isWarnEnabled()) {
					LOGGER.warn("Failed to process file \"" + filename + "\". Skipping file.", e);
				}

				throw e;
			}
		}

		return null;
	}
}
