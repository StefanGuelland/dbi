package dbi.benchmark;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import de.whs.dbi.loaddriver.Database;
import de.whs.dbi.loaddriver.ParameterGenerator;
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
	 * Benutzerdefinierter Konfigurationsparameter n
	 */
	protected final int n;

	protected CallableStatement  kontostand;
	protected CallableStatement  einzahlung;
	protected CallableStatement  analyse;
	
	/**
	 * Der Konstruktor initialisiert die Datenbankverbindung.
	 * 
	 * @param config Konfiguration
	 * @throws Exception 
	 */
	public TransactionsForMariaDB(Configuration config) throws Exception
	{
		super(config);

		// Liest einen benutzerdefinierten Parameter aus der Konfiguration aus
		n = Integer.parseInt(config.getBenchmarkProperty("user.n"));
		// Alle Transaktionen werden manuell commitet
		connection.setAutoCommit(false);
		
		kontostand = connection.prepareCall("{call kontostand(?, ?)}");
		kontostand.registerOutParameter(2, Types.NUMERIC);
		
		einzahlung = connection.prepareCall("{call einzahlung(?, ?, ?, ?, ?, ?)}");
		einzahlung.registerOutParameter(6, Types.NUMERIC);
		
		analyse = connection.prepareCall("{call analyse(?, ?)}");
		analyse.registerOutParameter(2, Types.NUMERIC);
	}

	/**
	 * Generiert zufaellige Eingeabeparameter und ruft die kontostandTX Funktion.
	 * @throws SQLException 
	 */
	public void kontostandTransaction() throws SQLException
	{
		int accountID = ParameterGenerator.generateRandomInt(1,n*100000);
		KontostandTX(accountID);
	}
	
	/**
	 * Generiert zufaellige Eingeabeparameter und ruft die einzahlungTX Funktion.
	 * @throws SQLException 
	 */	
	public void einzahlungTransaction() throws SQLException
    {
		int accountID = ParameterGenerator.generateRandomInt(1,n*100000);
		int tellerID = ParameterGenerator.generateRandomInt(1,n*10);
		int branchID = ParameterGenerator.generateRandomInt(1,n);
		int delta = ParameterGenerator.generateRandomInt(1,10000);
		String comment = ParameterGenerator.generateRandomString(30);
		
        einzahlungTX(accountID,tellerID,branchID,delta,comment);
    } 
	
	/**
	 * Generiert zufaellige Eingeabeparameter und ruft die analyseTX Funktion.
	 * @throws SQLException 
	 */	
	public void analyseTransaction() throws SQLException
	{
		int delta = ParameterGenerator.generateRandomInt(1,10000);
		analyseTX(delta);
	}
	
	public long KontostandTX(int accid) throws SQLException
	{	
		kontostand.setInt(1, accid);
		kontostand.execute();
        return kontostand.getInt(2);
	}
	
	public int einzahlungTX(int accid, int tellerid, int branchid, int delta, String comment) throws SQLException
    {
		einzahlung.setInt(1, accid);
		einzahlung.setInt(2, tellerid);
		einzahlung.setInt(3, branchid);
		einzahlung.setInt(4, delta);
		einzahlung.setString(5, comment);
		einzahlung.execute();
        return einzahlung.getInt(6);
    } 
	
	public int analyseTX(int delta) throws SQLException
	{
		analyse.setInt(1, delta);
		analyse.execute();
        return analyse.getInt(2);
	}

}
