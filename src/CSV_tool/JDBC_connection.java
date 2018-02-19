package CSV_tool;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JDBC_connection {
	
	public static Connection getConnection() throws SQLException {
		String user = "system";
	    String pass = "fjdos8Jps";
	    String url = "jdbc:oracle:thin:@localhost:1521:XE";
	    Locale.setDefault(Locale.getDefault());
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
	    Connection con = DriverManager.getConnection(url, user, pass);
		return con;
	}
	
	public static final Timestamp getTime() {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return timestamp;
	}
	
	public static final java.sql.Date frmtDate(String dtf) throws ParseException {
		SimpleDateFormat fmt = new SimpleDateFormat("dd.MM.yyyy");
		java.util.Date dat_utl;
      try {
		dat_utl=fmt.parse(dtf);
        java.sql.Date dat_sql = new java.sql.Date(dat_utl.getTime());
		return dat_sql;} catch (ParseException ex) {
			return null;}
	}
	
	public static void main(String[] args) throws SQLException, IOException, ParseException

	{

		int freq = Integer.parseInt("5000");
        while(true) {        
		if (Filecheck.chkfile() == true) {
	    	 	
	//	String csvFile = "/home/vitalii/Desktop/CSVparser/20130822_Organization_EGRUL.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";
	
	    String user = "system";
    String pass = "fjdos8Jps";
    String url = "jdbc:oracle:thin:@localhost:1521:XE";
    Locale.setDefault(Locale.getDefault());
       
    br = new BufferedReader(new FileReader (Filecheck.getFILENAME1()));
       
    FileWriter fw = new FileWriter(new File("/home/vitalii/Desktop/CSVparser/insert_error.txt"), true);

    String[] stolbec = new String[15];
    
    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
    Connection con = DriverManager.getConnection(url, user, pass);

//Statement st = con.createStatement(); //?

//insert data
String sqlPrepared = "INSERT INTO EGRUL_DATA_UPD (EGRPARTYID, INN, KPP, OGRN, FULLNAME, SHORTNAME, FISCALDATE, LIQSTATUS, RECSTATUS, RECSTATUSDATE, ACTDATE, UPDDATE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
PreparedStatement ps = con.prepareStatement(sqlPrepared);    
//Statement stf = con.createStatement();
String sqlPrepf = "INSERT INTO EGRUL_DATA_CLOSED_UPD (EGRPARTYID, INN, KPP, OGRN, FULLNAME, SHORTNAME, FISCALDATE, LIQSTATUS, RECSTATUS_, RECSTATUSDATE, ACTDATE, UPDDATE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
PreparedStatement psf = con.prepareStatement(sqlPrepf);
//mergestatement
String MDOPUL = "MERGE INTO EGRUL_DATA a USING "+
"EGRUL_DATA_UPD b "+
"ON (a.EGRPARTYID = b.EGRPARTYID) WHEN MATCHED THEN UPDATE SET "+
"a.INN = b.INN,"+
"a.KPP = b.KPP,"+
"a.OGRN = b.OGRN,"+
"a.FULLNAME = b.FULLNAME,"+
"a.SHORTNAME = b.SHORTNAME,"+
"a.FISCALDATE = b.FISCALDATE,"+
"a.LIQSTATUS = b.LIQSTATUS,"+
"a.RECSTATUS = b.RECSTATUS,"+
"a.RECSTATUSDATE = b.RECSTATUSDATE,"+
"a.ACTDATE = b.ACTDATE,"+
"a.UPDDATE = b.UPDDATE "+
"WHEN NOT MATCHED THEN "+
"INSERT (a.EGRPARTYID, a.INN, a.KPP, a.OGRN, a.FULLNAME, a.SHORTNAME, a.FISCALDATE, a.LIQSTATUS, a.RECSTATUS, a.RECSTATUSDATE, a.ACTDATE, a.UPDDATE) "+
"VALUES (b.EGRPARTYID, b.INN, b.KPP, b.OGRN, b.FULLNAME, b.SHORTNAME, b.FISCALDATE, "+
"b.LIQSTATUS, b.RECSTATUS, b.RECSTATUSDATE, b.ACTDATE, b.UPDDATE)";
PreparedStatement MDfOPUL = con.prepareStatement(MDOPUL); 

String MDCLUL = "MERGE INTO EGRUL_DATA_CLOSE a USING "+
"EGRUL_DATA_CLOSED_UPD b "+
"ON (a.EGRPARTYID = b.EGRPARTYID) WHEN MATCHED THEN UPDATE SET "+
"a.INN = b.INN,"+
"a.KPP = b.KPP,"+
"a.OGRN = b.OGRN,"+
"a.FULLNAME = b.FULLNAME,"+
"a.SHORTNAME = b.SHORTNAME,"+
"a.FISCALDATE = b.FISCALDATE,"+
"a.LIQSTATUS = b.LIQSTATUS,"+
"a.RECSTATUS_ = b.RECSTATUS_,"+
"a.RECSTATUSDATE = b.RECSTATUSDATE,"+
"a.ACTDATE = b.ACTDATE,"+
"a.UPDDATE = b.UPDDATE "+
"WHEN NOT MATCHED THEN "+
"INSERT (a.EGRPARTYID, a.INN, a.KPP, a.OGRN, a.FULLNAME, a.SHORTNAME, a.FISCALDATE, a.LIQSTATUS, a.RECSTATUS_, a.RECSTATUSDATE, a.ACTDATE, a.UPDDATE) "+
"VALUES (b.EGRPARTYID, b.INN, b.KPP, b.OGRN, b.FULLNAME, b.SHORTNAME, b.FISCALDATE, "+"b.LIQSTATUS, b.RECSTATUS_, b.RECSTATUSDATE, b.ACTDATE, b.UPDDATE)";
PreparedStatement MDfCLUL = con.prepareStatement(MDCLUL); 

String sqlDelTblopup = "DELETE FROM EGRUL_DATA_UPD";
PreparedStatement opup = con.prepareStatement(sqlDelTblopup);
String sqlDelTblclup = "DELETE FROM EGRUL_DATA_CLOSED_UPD";
PreparedStatement clup = con.prepareStatement(sqlDelTblclup);


    int cnt = 1;
    con.setAutoCommit(false);   
    boolean firstline = true;
    
     
    String substr=";";
    String flag = new String("True");
    String flf = new String("False");
    
    for (int i = 0; i<stolbec.length;i++){
    	while ((line = br.readLine()) != null)
    	{ if (firstline) {
    		firstline = false;
    		continue;
    	}
    	stolbec = line.split(cvsSplitBy); 
       //проверка на корректность строки (кол-во разделителей должно быть = 100) 
    	Pattern p = Pattern.compile(substr);
    	Matcher m = p.matcher(line);
    	int counter = 0;
    	while (m.find()){
    		counter++;
//    		System.out.println(counter);
    	}    	
    	if (counter == 100 && stolbec[24].equals(flag)) {    	   
    	//System.out.println(stolbec[24]);
    	//System.out.println(stolbec[0]+" "+stolbec[3]+" "+stolbec[5]+" "+stolbec[4]+" "+stolbec[7]+" "+stolbec[8]+" "+stolbec[6]+" "+stolbec[62]+" "+stolbec[56]+" "+stolbec[57]+" "+getTime()+" "+getTime());   	
    	ps.setString(1, stolbec[0]); // EGRPartyID
    	    ps.setString(2, stolbec[3]); // INN
    	    ps.setString(3, stolbec[5]); // KPP
    	    ps.setString(4, stolbec[4]); // OGRN
    	    ps.setString(5, stolbec[7]); // FullName
    	    ps.setString(6, stolbec[8]); // ShortName  
    	    ps.setDate(7, frmtDate(stolbec[6]));  // FiscalDate    	   
    	    ps.setString(8, stolbec[62]); // LiqStatus
    	    ps.setString(9, stolbec[57]); // RecStatus
    	    ps.setDate(10, frmtDate(stolbec[56])); // RecStatusDate
    	    ps.setTimestamp(11, getTime()); //
    	    ps.setTimestamp(12, getTime()); //
            ps.execute();
            System.out.println("Row " + cnt + " added to UPDATE EGRUL_DATA table");
            cnt++;      
            
            
            } 
    	
       else if (counter == 100 && stolbec[24].equals(flf))
    	{
    		psf.setString(1, stolbec[0]); // EGRPartyID
    	    psf.setString(2, stolbec[3]); // INN
    	    psf.setString(3, stolbec[5]); // KPP
    	    psf.setString(4, stolbec[4]); // OGRN
    	    psf.setString(5, stolbec[7]); // FullName
    	    psf.setString(6, stolbec[8]); // ShortName  
    	    psf.setDate(7, frmtDate(stolbec[6]));  // FiscalDate    	   
    	    psf.setString(8, stolbec[62]); // LiqStatus
    	    psf.setString(9, stolbec[57]); // RecStatus
    	    psf.setDate(10, frmtDate(stolbec[59])); // RecStatusDate
    	    psf.setTimestamp(11, getTime()); //
    	    psf.setTimestamp(12, getTime()); //
            psf.execute();

            System.out.println("Row " + cnt + " added to UPDATE EGRUL_DATA_CLOSE table");
            cnt++;
       //     st.close(); //???
    	}
    	
    	else {
            	System.out.println("Ошибка вставки. Не корректное количество разделитетелей в строке = "+counter+"");
            	fw.write("\n"+"Ошибка вставки. Кол-во разделителей: "+counter+" "+"Строка: "+stolbec[0]+" "+stolbec[3]+" "+stolbec[5]+" "+stolbec[4]+" "+stolbec[7]+" "+stolbec[8]+" "+stolbec[6]+" "+stolbec[62]+" "+stolbec[56]+" "+stolbec[57]+" "+getTime()+" "+getTime());
            	fw.flush();            
            }
    	}
    }
    con.commit();
    br.close();
    fw.close();  
     
 System.out.println("Начинаем merge для EGRUL_DATA");
    MDfOPUL.addBatch();
    MDfOPUL.executeBatch();
 System.out.println("Начинаем merge для EGRUL_DATA_CLOSED");   
 MDfCLUL.addBatch();
 MDfCLUL.executeBatch();
 con.commit();
 System.out.println("Начинается очистка временных таблиц");
 System.out.println("Очистка EGRUL_DATA_UPD");
 opup.addBatch();
 opup.executeBatch();
 System.out.println("Очистка EGRUL_DATA_CLOSED_UPD");
 clup.addBatch();
 clup.executeBatch();
 con.commit();
 con.close();
 System.out.println("Обновление данных завершено");
 break; 
    }
		else System.out.println("Файл с данными обновления не найден");
		try {
		     Thread.sleep((long)freq);
		 } catch (InterruptedException var9) {
		     var9.printStackTrace();
		 }
        }
	}
}

