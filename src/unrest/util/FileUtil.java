package unrest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class FileUtil {
	public static BufferedReader getFileReader(String path) {
		File localFile = new File(path);
		try {
			if (localFile.exists())
				return new BufferedReader(new FileReader(localFile));
		} catch (Exception e) { }
		return HadoopUtil.getFileReader(path);
	}
	
	public static BufferedReader getPropertiesReader() {
		BufferedReader localReader = getFileReader("unrest.properties");
		if (localReader != null)
			return localReader;
		else
			return getFileReader("/user/wmcdowell/osi/Projects/SocialUnrestPrediction/unrest.properties");
	}
}
