import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.file.io.CSVFileReader;
import com.file.io.CSVFileWriter;
import com.file.io.HTMLFileReader;
import com.file.merge.Merger;
import com.file.transform.InternalTable;
import com.file.type.InputFileType;
import com.file.type.OutputFileType;
import com.google.common.base.Strings;

/**
 * Merges one or more tables into one big table.
 *
 * 1. Validates provided files
 * 		- Ensures file is not a duplicate
 * 		- Ensures file type is supported
 *
 * 2. Parses each file into an internal data structure
 * 		- Merges the parsed table into a _merged_ table (also an internal data structure)
 *
 * 3. Writes the merged table to CSV 
 */
public class RecordMerger {
	// Constants

	/**
	 * The Logger instance
	 */
	private static final Log LOGGER = LogFactory.getLog(RecordMerger.class);

	/**
	 * Output filename
	 * 
	 * "combined.csv" by default
	 */
	private static final String FILENAME_COMBINED =
		System.getProperty("com.file.merger.outputFileName", "combined.csv");

	/**
	 * Name of the ID column
	 *
	 * "ID" by default
	 */
	public static final String ID_COLUMN_NAME =
		System.getProperty("com.file.merger.idColumnName", "ID");

	// Attributes

	/**
	 * Name of files to merge
	 */
	public static String[] mFilenames;

	// Constructor

	public RecordMerger(String... filenames) {
		mFilenames = filenames;

		if (Strings.isNullOrEmpty(FILENAME_COMBINED)) {
			throw new IllegalStateException("Output file name must be provided");
		}
	}

	// Operations

	/**
	 * Validates input files by ensuring the file type is supported.
	 */
	private HashMap<String, InputFileType.FileType> getValidFiles() {
		// Ensure filenames are unique, drop any duplicate files
		HashMap<String, InputFileType.FileType> fileTypeByFileNameMap =
			new HashMap<String, InputFileType.FileType>(1);

		for (int i = 0; i < mFilenames.length; i++) {
			String filename = mFilenames[i];

			if (!Strings.isNullOrEmpty(filename) && !fileTypeByFileNameMap.containsKey(filename)) {
				InputFileType.FileType fileType = InputFileType.extractFileType(filename);

				if (fileType == null || fileType == InputFileType.FileType.UNKNOWN) {
					if (LOGGER.isWarnEnabled()) {
						LOGGER.warn("Ignoring \"" + filename +
							"\" because either it has an unknown or missing file type");
					}
				}
				else {
					fileTypeByFileNameMap.put(filename, fileType);
				}
			}
		}

		return fileTypeByFileNameMap;
	}

	/**
	 * Parses input files using appropriate file processor.
	 */
	private InternalTable parse(String fileName, InputFileType.FileType fileType){
		InternalTable table = null;

		try {
			if (fileType == InputFileType.FileType.CSV) {
				// Assuming the name of the ID column is the same for all tables
				// In real world scenario, this should be configuration per table
				CSVFileReader csvReader = new CSVFileReader(ID_COLUMN_NAME);

				table = csvReader.process(fileName);
			}
			else if (fileType == InputFileType.FileType.HTML) {
				// Assuming the name of the ID column is the same for all tables
				// In real world scenario, this should be configuration per table
				HTMLFileReader htmlReader = new HTMLFileReader(ID_COLUMN_NAME);

				table = htmlReader.process(fileName);
			}
		}
		catch(IOException e) {
			LOGGER.info("Failed to parse file \"" + fileName + "\". Skipping it.", e);
		}

		return table;
	}

	/**
	 * Merges tables.
	 */
	private InternalTable merge() {
		HashMap<String, InputFileType.FileType> fileTypeByFileNameMap = getValidFiles();

		if (fileTypeByFileNameMap.isEmpty()) {
			LOGGER.info("No valid files are provided. Abandoning merge operation.");
		}
		else {
			// We probably should not output filenames as the customer may not want these in the
			// log messages for any developer, qa or support person to see
			LOGGER.info("Attempting to merge " + fileTypeByFileNameMap.size() + " files: " +
				fileTypeByFileNameMap.entrySet());

			Merger merger = new Merger();

			for (Map.Entry<String, InputFileType.FileType> fileTypeByFileNameEntry : fileTypeByFileNameMap.entrySet()) {
				String fileName = fileTypeByFileNameEntry.getKey();
				InputFileType.FileType fileType = fileTypeByFileNameEntry.getValue();

				InternalTable table = parse(fileName, fileType);

				try {
					merger.merge(table);
				}
				catch (IllegalStateException e) {
					if (LOGGER.isWarnEnabled()) {
						LOGGER.warn("Failed to merge table provided in file \"" + fileName + "\"", e);
					}
				}
			}

			return merger.getMergedTable();
		}

		return null;
	}

	/**
	 * Validates output file by ensuring the file type is supported.
	 */
	private boolean validateOutputFileType() {
		OutputFileType.FileType fileType = OutputFileType.extractFileType(FILENAME_COMBINED);

		if (fileType == null || fileType == OutputFileType.FileType.UNKNOWN) {
			LOGGER.warn("Either output file type \"" + fileType +
				"\" could not be determined or its not supported. Abandoning merge operations.");
		}
		else if (fileType == OutputFileType.FileType.CSV) {
			File file = new File(FILENAME_COMBINED);

			if (file.exists() && !file.canWrite()) {
				if (LOGGER.isWarnEnabled()) {
					LOGGER.warn("Output file \"" + FILENAME_COMBINED +
						"\" is write protected. Abandoning merge operations.");
				}
			}
			else {
				return true;
			}
		}

		return false;
	}

	/**
	 * Merges tables and uses appropriate file write to write the merged table to file.
	 */
	private void mergeTablesAndOutputToFile() {
		if (!validateOutputFileType()) {
			return;
		}

		InternalTable mergedTable = merge();

		if (mergedTable == null) {
			LOGGER.info("Merged table is empty.");
		}
		else {
			CSVFileWriter csvWriter = new CSVFileWriter(FILENAME_COMBINED, mergedTable);

			try {
				csvWriter.writeToFile();

				LOGGER.info("Merged files written to \"" + FILENAME_COMBINED + "\"");
			}
			catch (IOException e) {
				if (LOGGER.isWarnEnabled()) {
					LOGGER.warn("Error writing to output file \"" + FILENAME_COMBINED + "\"", e);
				}
			}
		}
	}

	/**
	 * Entry point of this test.
	 *
	 * @param args command line arguments: first.html and second.csv.
	 *
	 * @throws Exception bad things had happened.
	 */
	public static void main(final String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Usage: java RecordMerger file1 [ file2 [...] ]");

			System.exit(1);
		}

		// Assuming there is sufficient memory to store the tables in memory
		RecordMerger merger = new RecordMerger(args);

		merger.mergeTablesAndOutputToFile();
	}
}
