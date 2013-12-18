package dbi.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import de.whs.dbi.util.Configuration;

/**
 * MyInitDatabase ist eine weiter auszuprogrammierende Klasse zur Initialisierung der Datenbank.
 */
public class MariaDBInitDatabase 
{
	/**
	 * Datenbankverbindung
	 */
	protected static Connection connection;
	
	/**
	 * Konfiguration
	 */
	protected static Configuration config;
	
	/**
	 * Skalierungsfaktor fuer die Groesse der Datenbank
	 */
	protected static int n;
	
	/**
	 * Stellt eine Datenbankverbindung her und deaktiviert AutoCommit.
	 * Die ben�tigte JDBC-URL wird aus der Konfiguration gelesen.
	 * 
	 * @throws Exception 
	 */
	protected static void openConnection() throws Exception 
	{
		if ((config.issetBenchmarkProperty("database.jdbc.user")) && (config.issetBenchmarkProperty("database.jdbc.password")))
		{
			connection = DriverManager.getConnection(config.getBenchmarkProperty("database.jdbc.url"), config.getBenchmarkProperty("database.jdbc.user"), config.getBenchmarkProperty("database.jdbc.password"));
		}
		else
		{
			connection = DriverManager.getConnection(config.getBenchmarkProperty("database.jdbc.url"));
		}
		n = Integer.parseInt(config.getBenchmarkProperty("user.n"));
		connection.setAutoCommit(false);
	}
	
	/**
	 * Schlie�t die Datenbankverbindung.
	 * 
	 * @throws SQLException
	 */
	protected static void closeConnection() throws SQLException
	{
		connection.close();
	}
	
	/**
	 * L�scht, falls vorhanden, die bisherigen Tabellen.
	 * 
	 * @throws SQLException
	 */
	protected static void dropTables() throws SQLException
	{
		Statement statement = connection.createStatement();
		statement.executeUpdate("DROP TABLE IF EXISTS `history`;");
		statement.executeUpdate("DROP TABLE IF EXISTS `accounts`;");
		statement.executeUpdate("DROP TABLE IF EXISTS `tellers`; ");
		statement.executeUpdate("DROP TABLE IF EXISTS `branches`; ");
		statement.close();
	}
	
	/**
	 * Erstellt die Tabellen.
	 * 
	 * @throws SQLException
	 */
	protected static void createTables() throws SQLException 
	{
		Statement statement = connection.createStatement();
		statement.executeUpdate("CREATE TABLE IF NOT EXISTS `branches` ( "
				+ "  `branchid` int(11) NOT NULL, "
				+ "  `branchname` char(20) NOT NULL, "
				+ "  `balance` int(11) NOT NULL, "
				+ "  `address` char(72) NOT NULL, "
				+ "  PRIMARY KEY (`branchid`) "
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ");
		statement
				.executeUpdate("CREATE TABLE IF NOT EXISTS `accounts` ( "
						+ "  `accid` int(11) NOT NULL, "
						+ "  `name` char(20) NOT NULL, "
						+ "  `balance` int(11) NOT NULL, "
						+ "  `branchid` int(11) NOT NULL, "
						+ "  `address` char(68) NOT NULL, "
						+ "  PRIMARY KEY (`accid`), "
						+ "  KEY `branchid` (`branchid`), "
						+ "  CONSTRAINT `accounts_ibfk_1` FOREIGN KEY (`branchid`) REFERENCES `branches` (`branchid`) "
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ");
		statement
				.executeUpdate("CREATE TABLE IF NOT EXISTS `tellers` ( "
						+ "  `tellerid` int(11) NOT NULL, "
						+ "  `tellername` char(20) NOT NULL, "
						+ "  `balance` int(11) NOT NULL, "
						+ "  `branchid` int(11) NOT NULL, "
						+ "  `address` char(68) NOT NULL, "
						+ "  PRIMARY KEY (`tellerid`), "
						+ "  KEY `branchid` (`branchid`), "
						+ "  CONSTRAINT `tellers_ibfk_1` FOREIGN KEY (`branchid`) REFERENCES `branches` (`branchid`) "
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ");
		statement
				.executeUpdate("CREATE TABLE IF NOT EXISTS `history` ( "
						+ "  `accid` int(11) NOT NULL, "
						+ "  `tellerid` int(11) NOT NULL, "
						+ "  `delta` int(11) NOT NULL, "
						+ "  `branchid` int(11) NOT NULL, "
						+ "  `accbalance` int(11) NOT NULL, "
						+ "  `cmmnt` char(30) NOT NULL, "
						+ "  KEY `accid` (`accid`), "
						+ "  KEY `tellerid` (`tellerid`), "
						+ "  KEY `branchid` (`branchid`), "
						+ "  CONSTRAINT `history_ibfk_1` FOREIGN KEY (`accid`) REFERENCES `accounts` (`accid`), "
						+ "  CONSTRAINT `history_ibfk_2` FOREIGN KEY (`tellerid`) REFERENCES `tellers` (`tellerid`), "
						+ "  CONSTRAINT `history_ibfk_3` FOREIGN KEY (`branchid`) REFERENCES `branches` (`branchid`), "
						+ "  INDEX(delta)"
						+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
		statement.close();
	}

	/**
	 * F�hrt optimierende Konfigurationseinstellungen im DBMS
	 * und an der Datenbank durch.
	 * 
	 * @throws SQLException
	 */
	protected static void tuneDatabase() throws SQLException 
	{
		Statement statement = connection.createStatement();
		statement.execute("drop procedure if exists `kontostand`;");
		statement.execute("drop procedure if exists `einzahlung`;");
		statement.execute("drop procedure if exists `analyse`;");
		statement.execute(
				"CREATE  PROCEDURE `kontostand`(IN accid_ INT, OUT balance_ INT) " +
				"BEGIN " +
				"SELECT balance FROM accounts WHERE accid=accid_ INTO balance_; " +
				"END"
				);
		statement.execute(
				"CREATE PROCEDURE `einzahlung`(IN accid_ INT, IN tellerid_ INT, IN branchid_ INT, IN delta_ INT, IN comment_ char(30), OUT newBalance INT) " +
				"BEGIN " +
				"START TRANSACTION; " +
				"UPDATE branches SET balance=balance + delta_ WHERE branchid=branchid_; " +
				"UPDATE tellers SET balance=balance + delta_ WHERE tellerid=tellerid_; " +
				"UPDATE accounts SET balance=balance + delta_ WHERE accid=accid_; " +
				"SELECT balance FROM accounts WHERE accid=accid_ INTO newBalance; " +
				"INSERT INTO history VALUES (accid_,tellerid_,delta_,branchid_,newBalance, comment_); " +
				"COMMIT; " +
				"END"
				);
		statement.execute(
				"CREATE PROCEDURE `analyse`(IN delta_ INT, OUT count_ INT) " +
				"BEGIN " +
				"SELECT count(*) FROM history WHERE delta=delta_ INTO count_; " +
				"END"
				);
		statement.close();
	}

	/**
	 * Legt die neuen Datens�tze an.
	 * 
	 * @throws SQLException
	 */
	protected static void insertRows() throws SQLException 
	{
		Statement statement = connection.createStatement();
		Random randomGenerator = new Random();
		int randomNumber;

		connection.setAutoCommit(false);
		statement.executeUpdate("SET foreign_key_checks=0;");

		for (int i = 1; i <= n; i++)
			statement
					.executeUpdate("INSERT INTO branches  VALUES ( "
							+ i
							+ " , 'qwertyuiopqwertyuiop', 0, 'qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqw');");
		long nFactor = n * 100000;
		for (int i = 1; i < nFactor; i += 100) {
			randomNumber = randomGenerator.nextInt(n) + 1;
			StringBuffer request = new StringBuffer(
					"INSERT INTO accounts  VALUES ");
			request.append(" ("
					+ i
					+ ", 'qwertyuiopqwertyuiop', 0, "
					+ randomNumber
					+ ", 'qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyui')");
			for (int j = 1; j < 100; j++) {
				randomNumber = randomGenerator.nextInt(n) + 1;
				request.append(",("
						+ (j + i)
						+ ", 'qwertyuiopqwertyuiop', 0, "
						+ randomNumber
						+ ", 'qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyui')");
			}
			request.append(";");
			statement.executeUpdate(request.toString());
		}
		nFactor = n * 10;
		for (int i = 1; i <= nFactor; i++) {
			randomNumber = randomGenerator.nextInt(n) + 1;
			statement
					.executeUpdate("INSERT INTO tellers  VALUES ("
							+ i
							+ ", 'qwertyuiopqwertyuiop', 0, "
							+ randomNumber
							+ ", 'qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyui');");
		}
		connection.commit();
		statement.executeUpdate("SET foreign_key_checks=1;");
		statement.close();
	}

	/**
	 * Hauptprogramm zum Starten der Initialisierung der Datenbank.
	 * 
	 * @param args
	 */
	public static void main(String[] args)
	{	
		try
		{
			System.out.println("Initialisierung der Datenbank:\n");

			// L�dt die Konfiguration
			config = new Configuration();
			config.loadBenchmarkConfiguration();
			
			openConnection();
			System.out.println("L�schen vorhandener Relationen ...");
			dropTables();
			System.out.println("Anlegen des Datenbankschemas ...");
			createTables();
			System.out.println("Durchf�hrung von Tuning-Einstellungen ...");
			tuneDatabase();
			System.out.println("Einf�gen der Tupel ...");
			
			// Merken des Start-Zeitpunktes der Initialisierung
			long duration = System.currentTimeMillis();
			
			insertRows();
			
			// Berechnung der Zeitdauer der Initialisierung
			duration = System.currentTimeMillis()-duration;
			
			closeConnection();

			System.out.println("Einf�gedauer: " + duration/1000 + " Sekunden\n");
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
