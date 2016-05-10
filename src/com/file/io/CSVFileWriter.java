package com.file.io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.file.transform.InternalTable;
import com.google.common.collect.Iterables;

import au.com.bytecode.opencsv.CSVWriter;

public class CSVFileWriter {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(CSVFileReader.class);

	// Attributes

	private String mOutputFilename;

	// Associations

	private InternalTable mTable;

	// Constructors

	public CSVFileWriter(String filename, InternalTable table) {
		mOutputFilename = filename;
		mTable = table;
	}

	public void writeToFile() throws IOException {
		File output = new File(mOutputFilename);

		if (output.exists() && !output.canWrite()) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("Unable to write to file \"" + mOutputFilename + "\". Skipping writing to file.");
			}

			return;
		}

		CSVWriter writer = new CSVWriter(new FileWriter(output), ',');

		writer.writeNext(Iterables.toArray(mTable.getColumnNameSet(), String.class));

		for (Iterator<List<String>> rowItr = mTable.getRowItr(); rowItr.hasNext(); ) {
			writer.writeNext(Iterables.toArray(rowItr.next(), String.class));
		}

		writer.close();
	}
}
