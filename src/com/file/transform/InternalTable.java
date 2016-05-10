package com.file.transform;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Strings;


/**
 * Internal table.
 */
public class InternalTable {
	// Constants

	/**
	 * The Logger instance.
	 */
	private static final Log LOGGER = LogFactory.getLog(InternalTable.class);

	// Attributes

	private int mNumCols;

	/**
	 * Index of the ID column in the table. 
	 */
	private int mIDIdx = -1;

	/**
	 * Name of the ID column.
	 */
	private String mIdColumnName;

	// Associations

	/**
	 * Map of rows containing Set of columns indexed by primary key.
	 *
	 * Must be able to grow in size to accommodate for initial table creation step and merge
	 * operations.
	 * 
	 * Must sort data as it is put into the table.
	 */
	private TreeMap<String, List<String>> mDataByID;

	/**
	 * Map of column index, indexed by column name.
	 * 
	 * Must have predictable iteration order.
	 */
	private LinkedHashMap<String, Integer> mColIdxByNameMap;

	// Constructors

	public InternalTable(List<String> colNameList, String idColumnName) {
		if (colNameList == null) {
			throw new IllegalStateException("Must provide a header.");
		}

		int numCols = colNameList.size();

		if (numCols <= 0) {
			throw new IllegalStateException("Number of columns is invalid.");
		}
		else if (Strings.isNullOrEmpty(idColumnName)) {
			throw new IllegalStateException("Name of the ID column must be provided.");
		}

		mNumCols = numCols;
		mIdColumnName = idColumnName;
		mColIdxByNameMap = new LinkedHashMap<String, Integer>();

		addColumnNameRow(colNameList, idColumnName);

		mDataByID = new TreeMap<String, List<String>>();

		if (mIDIdx == -1) {
			throw new IllegalStateException("ID column not found in the table.");
		}
	}

	// Operations

	/**
	 * Add row of column names to the internal representation of the table.
	 *
	 * @param colName List Column name row.
	 * @param idColumnName Name of the ID column.
	 */
	private void addColumnNameRow(List<String> colNameList, String idColumnName) {
		if (colNameList != null) {
			if (mNumCols == colNameList.size()) {
				// we should determine the index for the ID column
				// and ensure column names are unique
				if (!Strings.isNullOrEmpty(idColumnName)) {
					
					for (int i = 0; i < colNameList.size(); i++) {
						String colName = colNameList.get(i);

						if (mColIdxByNameMap.containsKey(colName)) {
							throw new IllegalStateException("Column name \"" + colName +
								"\" is not unique.");
						}

						if (idColumnName.equalsIgnoreCase(colName)) {
							mIDIdx = i;
							mIdColumnName = idColumnName;
						}

						mColIdxByNameMap.put(colName, i);
					}
				}
			}
			else {
				throw new IllegalStateException(
					"Skipping column name row as it does not contain specified number of columns.");
			}
		}
	}

	/**
	 * Adds a new column.
	 *
	 * @param colName The name of the column to add.
	 *
	 * @return True, if the new column was added to the table. False, if the column already exists
	 * or if colName is null or empty string.
	 */
	public boolean addColumn(String colName) {
		if (!Strings.isNullOrEmpty(colName)) {
			if (!mColIdxByNameMap.containsKey(colName)) {
				mColIdxByNameMap.put(colName, mNumCols);

				mNumCols++;

				for (List<String> colDataList : mDataByID.values()) {
					colDataList.add(null);
				}

				return true;
			}
			else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Column name \"" + colName + "\" already exists.");
				}
			}
		}
		else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Must provide a column name.");
			}
		}

		return false;
	}

	/**
	 * Adds data for the row indexed by provided id.
	 *
	 * @param id The row index id.
	 * @param colName The name of the column to add the data to.
	 * @param value The data to set.
	 *
	 * @return True, if adding data was successful. Otherwise, false.
	 */
	public boolean addRowData(String id, String colName, String value) {
		if (Strings.isNullOrEmpty(id)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Must provide an id for the row.");
			}
		}
		else if (Strings.isNullOrEmpty(colName)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Must provide the column name.");
			}
		}
		else {
			List<String> rowDataList = mDataByID.get(id);
			int colIdx = mColIdxByNameMap.get(colName);

			if (rowDataList == null) {
				rowDataList = new ArrayList<String>(mNumCols);

				for (int i = 0; i < mNumCols; i++) {
					String valueToSet = null;

					if (i == mIDIdx) {
						valueToSet = id;
					}
					else if (i == colIdx) {
						valueToSet = value;
					}

					rowDataList.add(i, valueToSet);
				}
			}
			else {
				// do not replace existing value in the column unless its null or empty string
				// merged tables gets priority over new table being merged in
				// for instance, if merged table contains column "name" and data "Homer"
				// new table has the same column "name" and data "Bart"
				// In this case, "Homer" wins this conflict
				// if merged table contains an empty string or null value for "name" column,
				// then, this null or empty string in the merged table will be replaced by
				// data from the new table
				if (Strings.isNullOrEmpty(rowDataList.get(colIdx))) {
					rowDataList.set(colIdx, value);
				}
			}

			mDataByID.put(id, rowDataList);

			return true;
		}

		return false;
	}

	/**
	 * Add row of data to the internal representation of the table.
	 *
	 * @param dataRow The data row.
	 */
	public void addData(List<String> dataRow) {
		if (dataRow != null) {
			int rowSize = dataRow.size();

			if (mNumCols == rowSize) {
				List<String> colDataList = new ArrayList<String>(mNumCols);
				String id = null;

				for (int i = 0; i < rowSize; i++) {
					String colData = dataRow.get(i);

					if (i == mIDIdx) {
						if (Strings.isNullOrEmpty(colData)) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Column ID must not be null. Skipping row.");
							}

							continue;
						}

						if (mDataByID.containsKey(colData)) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Duplicate primary key \"" + colData + "\" found for. Skipping row");
							}

							continue;
						}

						id = colData;
					}
					
					colDataList.add(colData);
				}

				mDataByID.put(id, colDataList);
			}
			else {
				// do not abandon execution if one row is smaller or larger in size
				if (LOGGER.isWarnEnabled()) { // may not want to log table data due to customer data confidentiality concerns
					LOGGER.warn("Skipping data row as it does not contain required number of columns.\n" + dataRow);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		// Print column names
		for (Iterator<String> colNameItr = mColIdxByNameMap.keySet().iterator(); colNameItr.hasNext();) {
			String colName = colNameItr.next();

			sb.append(colName);

			if (colNameItr.hasNext()) {
				sb.append("\t");
			}
		}

		sb.append("\n");

		// Print data
		for (Iterator<List<String>> rowItr = mDataByID.values().iterator(); rowItr.hasNext();) {
			Iterator<String> colItr = rowItr.next().iterator();

			for (; colItr.hasNext(); ) {
				sb.append(colItr.next());

				if (colItr.hasNext()) {
					sb.append("\t");
				}
			}

			if (rowItr.hasNext()) {
				sb.append("\n");
			}
		}

		return sb.toString();
	}

	public int getIdColumnIndex() {
		return mIDIdx;
	}

	public int getColumnIndex(String colName) {
		Integer index = mColIdxByNameMap.get(colName);

		return (index != null) ? index : -1;
	}

	public Set<String> getColumnNameSet() {
		return mColIdxByNameMap.keySet();
	}

	public Iterator<List<String>> getColumnItr(final String colName) {
		return new Iterator<List<String>>() {
			final Iterator<List<String>> rowItr = mDataByID.values().iterator();

			@Override
			public boolean hasNext() {
				return rowItr.hasNext();
			}

			@Override
			public List<String> next() {
				if (hasNext()) {
					List<String> colList = rowItr.next();

					return colList;
				}

				return null;
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Removing data is not permitted.");
			}
		};
	}

	public String getIDColumnName() {
		return mIdColumnName;
	}

	public Iterator<List<String>> getRowItr() {
		return new Iterator<List<String>>() {
			final Iterator<Entry<String, List<String>>> mDataByIDEntryItr = mDataByID.entrySet().iterator();

			@Override
			public boolean hasNext() {
				return mDataByIDEntryItr.hasNext();
			}

			@Override
			public List<String> next() {
				if (hasNext()) {
					return mDataByIDEntryItr.next().getValue();
				}

				return null;
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Removing data is not permitted.");
			}
		};
	}
}
