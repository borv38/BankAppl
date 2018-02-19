package CSV_tool;

import java.io.File;

public class Filecheck {
	 //добавить формирование наименования файла по дате
	public static final String FILENAME1 = "/home/vitalii/Desktop/CSVparser/20130823_Organization_EGRUL.csv";
	
	public static boolean chkfile() {
        return checkFile(getFILENAME1());
    }
	
	private static boolean checkFile(String filename) {
        final File file = new File(filename);
        return file.exists();
	}

	public static String getFILENAME1() {
		return FILENAME1;
	}

}
