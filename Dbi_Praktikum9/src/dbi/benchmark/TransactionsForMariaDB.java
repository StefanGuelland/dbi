package dbi.benchmark;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.whs.dbi.loaddriver.Database;
import de.whs.dbi.util.Configuration;

/**
 * MyTransactionsForMyDBMS ist eine weiter auszuprogrammierende Unterklasse von
 * Database. Diese Klasse enth�lt die Benchmark-spezifischen Lasttransaktionen
 * sowie die DBMS-spezifischen Anpassungen bei �berschriebenen Methoden aus
 * Database.
 * Lasttransaktionen sind als parameterlose public-void-Methoden zu realisieren.
 */
public class TransactionsForMariaDB extends Database
{
	/**
	 * Der Konstruktor initialisiert die Datenbankverbindung.
	 * 
	 * @param config Konfiguration
	 * @throws Exception 
	 */
	public TransactionsForMariaDB(Configuration config) throws Exception
	{
		super(config);
	}

	/**
	 * Eine Transaktion, die in der Konfiguration als Beispiel definiert ist.
	 */
	public void kontostandTransaction()
	{
		try {
			KontostandTX("22");
		} catch (SQLException e) {
			// TODO Automatisch generierter Erfassungsblock
			e.printStackTrace();
		}
	}
	
	public void einzahlungTransaction()
	{
		
	}
	
	public void analyseTransaction()
	{
		try {
			analyseTX(100);
		} catch (SQLException e) {
			// TODO Automatisch generierter Erfassungsblock
			e.printStackTrace();
		}
	}
	
	public long KontostandTX(String accid) throws SQLException
	{
		long balance = 0;
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery("SELECT balance FROM accounts WHERE accid=" + accid + ";");
		if(result.next())
			balance = result.getLong(1);
		statement.close();
		return balance;
		
	}
	
	public void einzahlungTX(String accid, String tellerid, String branchid, String delta) throws SQLException
	{
		Statement statement = connection.createStatement();
		statement.executeQuery("SELECT balance FROM branches WHERE branchid=4;");
		statement.close();
	}
	
	public void analyseTX(long delta) throws SQLException
	{
		Statement statement = connection.createStatement();
		statement.executeQuery("SELECT count(*) FROM history WHERE delta=" + delta + ";");
		statement.close();
	}

}
