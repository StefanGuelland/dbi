package dbi.benchmark;

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
	
	public void KontostandTX(String accid) throws SQLException
	{
		Statement statement = connection.createStatement();
		statement.executeQuery("SELECT balance FROM accounts WHERE accid=" + accid + ";");
		statement.close();
		
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
