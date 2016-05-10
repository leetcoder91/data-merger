package com.file.type;

import com.google.common.base.Strings;
import com.google.common.io.Files;

public class InputFileType {
	public enum FileType {
	    HTML,
	    CSV,
	    UNKNOWN
	}

	public static InputFileType.FileType extractFileType(String filename) {
		String fileType = Files.getFileExtension(filename);

		if (!Strings.isNullOrEmpty(fileType)) {
			if ("HTML".equalsIgnoreCase(fileType)) {
				return FileType.HTML;
			}
			else if ("CSV".equalsIgnoreCase(fileType)) {
				return FileType.CSV;
			}
		}

		return FileType.UNKNOWN;
	}
}
