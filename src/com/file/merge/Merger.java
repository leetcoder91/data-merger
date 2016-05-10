package com.file.merge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.file.transform.InternalTable;

public class Merger {
	// Associations

	/**
	 * Internal merged table.
	 */
	private InternalTable mMergedTable;

	// Operations

	/**
	 * Merges provided table with internal merged table.
	 *
	 * If there are new columns in the provided table that does not exist in the merged table,
	 * the new column and data in this column is copied over from the provided table to the merged
	 * table.
	 * 
	 * If provided table contains the same column(s) as the merged table, then data (not null only)
	 * in the merged table is not overwritten with the data in the provided table.
	 * 
	 * Instead of having one hardcoded merging policy defined, we should allow the user to provide
	 * a merging policy in the format we can understand that defines how one the merge should
	 * proceed in case of conflicts.
	 * 
	 * @param table The table to merge.
	 */
	public void merge(InternalTable table) {
		if (table == null) {
			return;
		}

		Set<String> colNameSet = table.getColumnNameSet();

		if (mMergedTable == null) {
			mMergedTable = new InternalTable(new ArrayList<String>(colNameSet), table.getIDColumnName());

			for (Iterator<List<String>> rowItr = table.getRowItr(); rowItr.hasNext(); ) {
				mMergedTable.addData(rowItr.next());
			}
		}
		else {
			Set<String> mergedTableColNameSet = mMergedTable.getColumnNameSet();
			int idIdx = table.getIdColumnIndex();

			for (String colName : colNameSet) {
				if (!mergedTableColNameSet.contains(colName)) {
					mMergedTable.addColumn(colName);
				}

				int colIdx = table.getColumnIndex(colName);

				for (Iterator<List<String>> colDataItr = table.getColumnItr(colName); colDataItr.hasNext(); ) {
					List<String> colDataList = colDataItr.next();

					mMergedTable.addRowData(colDataList.get(idIdx), colName, colDataList.get(colIdx));
				}
			}
		}
	}

	/**
	 * @return The merged table.
	 */
	public InternalTable getMergedTable() {
		return mMergedTable;
	}
}
