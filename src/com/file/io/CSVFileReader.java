package com.file.io;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.file.transform.InternalTable;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads a CSV file and creates a corresponding internal table for storing the table data.
 */
public class CSVFileReader extends AbstractFileReader {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(CSVFileReader.class);

	// Constructors

	public CSVFileReader(String idColumnName) {
		super(idColumnName);
	}

	// Operations

	public InternalTable process(String filename) throws IOException {
		File input = getFile(filename);

		if (input != null) {
			CSVReader reader = new CSVReader(new FileReader(input));
			String [] nextDataRow = reader.readNext();
			InternalTable internalTable = null;

			if (nextDataRow != null) {
				List<String> dataList = new ArrayList<String>(nextDataRow.length);

				for (String colData : nextDataRow) {
					dataList.add(colData);
				}

				internalTable = new InternalTable(dataList, getIdColumnName());
			}

			while ((nextDataRow = reader.readNext()) != null) {
				try {
					internalTable.addData(Arrays.asList(nextDataRow));
				}
				catch (IllegalStateException e) {
					if (LOGGER.isWarnEnabled()) {
						LOGGER.warn("Failed to process file \"" + filename + "\". Skipping file.", e);
					}

					throw e;
				}
			}

			return internalTable;
		}

		return null;
	}
}
