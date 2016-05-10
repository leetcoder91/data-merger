package com.file.io;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Strings;

/**
 * Default file operations.
 */
public class AbstractFileReader {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(AbstractFileReader.class);

	// Attributes

	/**
	 * Name of the ID column.
	 */
	protected final String mIdColumnName;

	// Constructors

	public AbstractFileReader(String idColumnName) {
		if (Strings.isNullOrEmpty(idColumnName)) {
			throw new IllegalStateException("Name of the ID column must be provided");
		}

		mIdColumnName = idColumnName;
	}

	// Operations

	public File getFile(String filename) throws IOException {
		File input = new File(filename);

		if (!input.exists()) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("\"" + filename + "\" does not exist. Skipping it");
			}

			return null;			
		}
		else if (!input.canRead()) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("\"" + filename + "\" is read protected. Skipping it");
			}

			return null;
		}

		return input;
	}

	public String getIdColumnName() {
		return mIdColumnName;
	}
}
