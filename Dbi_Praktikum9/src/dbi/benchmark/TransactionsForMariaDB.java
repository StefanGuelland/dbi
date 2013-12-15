package dbi.benchmark;

import java.sql.PreparedStatement;
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
	
	protected PreparedStatement selectBalanceAccounts;
	
	protected PreparedStatement einzahlungTXSelectBalanceFromBranches;
	protected PreparedStatement einzahlungTXUpdateBalanceFromBranches;	
	protected PreparedStatement einzahlungTXSelectBalanceFromTellers;
	protected PreparedStatement einzahlungTXUpdateBalanceFromTellers;
	protected PreparedStatement einzahlungTXSelectBalanceFromAccounts;
	protected PreparedStatement einzahlungTXInsertHistory;
	protected PreparedStatement einzahlungTXUpdateBalanceFromAccounts;
	
	
	protected PreparedStatement analyseTXSelectDelta;
	
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
		selectBalanceAccounts = connection.prepareStatement("SELECT balance FROM accounts WHERE accid=?;");
		einzahlungTXSelectBalanceFromBranches = connection.prepareStatement("SELECT balance FROM branches WHERE branchid=?;");
		einzahlungTXUpdateBalanceFromBranches = connection.prepareStatement("UPDATE branches SET balance=? WHERE branchid=?;");
		einzahlungTXSelectBalanceFromTellers = connection.prepareStatement("SELECT balance FROM tellers WHERE tellerid=?;");
		einzahlungTXUpdateBalanceFromTellers = connection.prepareStatement("UPDATE tellers SET balance=? WHERE tellerid=?;");
		einzahlungTXSelectBalanceFromAccounts = connection.prepareStatement("SELECT balance FROM accounts WHERE accid=?;");
		einzahlungTXUpdateBalanceFromAccounts = connection.prepareStatement("UPDATE accounts SET balance=? WHERE accid=?;");
		einzahlungTXInsertHistory = connection.prepareStatement("INSERT INTO history VALUES (?,?,?,?,?,?)");
		analyseTXSelectDelta = connection.prepareStatement("SELECT count(*) FROM history WHERE delta=?;");
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
		selectBalanceAccounts.setInt(1, accid);
		
		//Statement statement = connection.createStatement();
		ResultSet result =  selectBalanceAccounts.executeQuery();
		if(result.next())
			balance = result.getLong(1);
		//statement.close();
		return balance;
		
	}
	
	public int einzahlungTX(int accid, int tellerid, int branchid, int delta) throws SQLException
    {
        /**
         * balance in branches mit der speziellen branchid aktualisieren
         */
        einzahlungTXSelectBalanceFromBranches.setInt(1, branchid);
        ResultSet res = einzahlungTXSelectBalanceFromBranches.executeQuery();

        /* Abfrage ob ein Eintrag verfügbar ist. */
        if(res.next()){
          
        	einzahlungTXUpdateBalanceFromBranches.setInt(1, delta + res.getInt(1));
        	einzahlungTXUpdateBalanceFromBranches.setInt(2, branchid);
        	einzahlungTXUpdateBalanceFromBranches.executeUpdate();
        }
        else
            return 0; /* Rückgabe bei Fehler in Datenbank - ÄNDERN -> throw? return 0? */
          
        /**
         * balance in tellers mit der speziellen tellerid aktualisieren
         */
        einzahlungTXSelectBalanceFromTellers.setInt(1, tellerid);
        res = einzahlungTXSelectBalanceFromTellers.executeQuery();
          
        if(res.next()){
        	einzahlungTXUpdateBalanceFromTellers.setInt(1,delta+res.getInt(1));
        	einzahlungTXUpdateBalanceFromTellers.setInt(2, tellerid);
        	einzahlungTXUpdateBalanceFromTellers.executeUpdate();
        }
        else
            return 0;
          
        /**
         * balance in accounts mit der speziellen accid aktualisieren
         */
        einzahlungTXSelectBalanceFromAccounts.setInt(1, tellerid);
        res = einzahlungTXSelectBalanceFromAccounts.executeQuery();
          
        if(res.next()){
            einzahlungTXUpdateBalanceFromAccounts.setInt(1,delta+res.getInt(1));
        	einzahlungTXUpdateBalanceFromAccounts.setInt(2, accid);
        	einzahlungTXUpdateBalanceFromAccounts.executeUpdate();
            
        }
        else
            return 0;
        einzahlungTXInsertHistory.setInt(1, accid);
        einzahlungTXInsertHistory.setInt(2, tellerid);
        einzahlungTXInsertHistory.setInt(3, delta);
        einzahlungTXInsertHistory.setInt(4, branchid);
        einzahlungTXInsertHistory.setInt(5, res.getInt(1));
        einzahlungTXInsertHistory.setString(6,"lala");
        einzahlungTXInsertHistory.executeUpdate();
       
        connection.commit();
        return delta+res.getInt(1);
    } 
	
	public int analyseTX(int delta) throws SQLException
	{
		int count = 0;
		analyseTXSelectDelta.setInt(1, delta);
		ResultSet result = analyseTXSelectDelta.executeQuery();
		if(result.next())
			count = result.getInt(1);
		return count;
		
	}

}
