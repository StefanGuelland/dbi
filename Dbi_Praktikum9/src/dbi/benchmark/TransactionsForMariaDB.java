package dbi.benchmark;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
		
        int kontostand=einzahlungTX(accountID,tellerID,branchID,delta);
              
            //System.out.println(kontostand);
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
		long balance = 0;
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery("SELECT balance FROM accounts WHERE accid=" + accid + ";");
		if(result.next())
			balance = result.getLong(1);
		statement.close();
		return balance;
		
	}
	
	public int einzahlungTX(int accid, int tellerid, int branchid, int delta) throws SQLException
    {
        Statement statement = connection.createStatement();
        /**
         * balance in branches mit der speziellen branchid aktualisieren
         */
        ResultSet res = statement.executeQuery("SELECT balance FROM branches WHERE branchid='" + branchid + "'");
          
        /* Abfrage ob ein Eintrag verfügbar ist. */
        if(res.next()){
          
            statement.executeQuery("UPDATE branches SET balance=" + (delta+res.getInt(1)) + " WHERE branchid='" + branchid + "';");
        }
        else
            return 0; /* Rückgabe bei Fehler in Datenbank - ÄNDERN -> throw? return 0? */
          
        /**
         * balance in tellers mit der speziellen tellerid aktualisieren
         */
        res = statement.executeQuery("SELECT balance FROM tellers WHERE tellerid='" + tellerid + "';");
          
        if(res.next()){
            statement.executeQuery("UPDATE tellers SET balance=" + (delta+res.getInt(1)) + " WHERE tellerid='" + tellerid + "';");
        }
        else
            return 0;
          
        /**
         * balance in accounts mit der speziellen accid aktualisieren
         */
        res = statement.executeQuery("SELECT balance FROM accounts WHERE accid='" + accid + "';");
          
        if(res.next()){
            statement.executeQuery("UPDATE accounts SET balance=" + (delta+res.getInt(1)) + " WHERE accid='" + accid + "';");
        }
        else
            return 0;
        statement.executeQuery("INSERT INTO history VALUES ('"+ accid +"','"+ tellerid +"',"+ delta +",'"+ branchid +"','"+ res.getInt(1) +"','"+ "x" +"')");//cmmnt char
          
        /**
         * akutelles balance aus der datenbank ziehen
         */
        res = statement.executeQuery("SELECT balance FROM accounts WHERE accid='" + accid + "';");
        res.next();
          
        statement.close();
        connection.commit();
        return res.getInt(1);
    } 
	
	public void analyseTX(long delta) throws SQLException
	{
		Statement statement = connection.createStatement();
		statement.executeQuery("SELECT count(*) FROM history WHERE delta=" + delta + ";");
		statement.close();
	}

}
