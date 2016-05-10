package com.file.type;

import com.google.common.base.Strings;
import com.google.common.io.Files;

public class OutputFileType {
	public enum FileType {
	    CSV,
	    UNKNOWN
	}

	public static OutputFileType.FileType extractFileType(String filename) {
		String fileType = Files.getFileExtension(filename);

		if (!Strings.isNullOrEmpty(fileType)) {
			if ("CSV".equalsIgnoreCase(fileType)) {
				return FileType.CSV;
			}
		}

		return FileType.UNKNOWN;
	}
}
