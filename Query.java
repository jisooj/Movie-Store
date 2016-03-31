/*
	CSE 414 
	Homework 7
	Jisoo Jung
	
	Query contains approximately 20 different queries to manage 
	movie renting store program that uses IMDB and Customer databases
	to store all the movie-related and customer information.
	Query comes with functionalities that prevents the user from 
	having an odd issues caused by renting and returning at the same time.

*/

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;
	private String jSQLCustomerUrl;

	// DB Connection
	private Connection conn;
    private Connection customerConn;

	// Canned queries

	// LIKE does a case-insensitive match
	/*
	private static final String SEARCH_SQL_BEGIN =
		"SELECT * FROM movie WHERE name LIKE '%";
	private static final String SEARCH_SQL_END = 
		"%' ORDER BY id";
	*/
	private static final String SEARCH_SQL_INJECTION_FREE = 
		"SELECT * FROM movie WHERE name LIKE ? ORDER BY id";
	private PreparedStatement searchStatementInjectionFree;
		
	private static final String DIRECTOR_MID_SQL = "SELECT y.* "
					 + "FROM movie_directors x, directors y "
					 + "WHERE x.mid = ? and x.did = y.id";
	private PreparedStatement directorMidStatement;

	private static final String CUSTOMER_LOGIN_SQL = 
		"SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement customerLoginStatement;

	private static final String BEGIN_TRANSACTION_SQL = 
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;
	
	// Task 2A: Search function 
	// finding actors by movie name does not work, use mid instead
	private static final String ACTOR_SQL = 
		"SELECT a.* " + 
		"FROM actor a, casts c " +
		"WHERE a.id = c.pid " + 
		"AND c.mid = ?";
	private PreparedStatement actorMnameStatement;

	// open means rented, closed means returned
	private static final String CURRENT_RENTAL =
		"SELECT cid " + 
		"FROM Rental " +
		"WHERE mid = ?";
	private PreparedStatement currentRentalStatement;
	
	//-------------------------------------------------
	private static final String REMAINING_RENTAL_SQL = 
		"SELECT (SELECT p.maxRental " + 
		  "FROM Plans p " +
		  "WHERE p.pid = c.pid) - " + 
		  "(SELECT count(*) " +
		  "FROM Rental r " +
		  "WHERE c.cid = r.cid " +
		  "AND r.status = 'open') AS available " +
		"FROM Customer c " +
		"WHERE c.cid = ?";
	private PreparedStatement remainingRentalStatement;

	private static final String CUSTOMER_NAME_SQL = 
		"SELECT firstName, lastName " + 
		"FROM Customer " + 
		"WHERE cid = ?";
	
	private PreparedStatement customerNameStatement;

	private static final String VALID_PLAN_SQL = 
		"SELECT count(*) AS planCount " + 
		"FROM Plans " + 
		"WHERE pid = ?";
	private PreparedStatement validPlanStatement;
	
	private static final String VALID_MOVIE_SQL = 
		"SELECT count(*) AS movieCount " + 
		"FROM Movie " + 
		"WHERE id = ?";
	private PreparedStatement validMovieStatement;
	
	private static final String MAX_RENTAL = 
		"SELECT maxRental " + 
		"FROM Plans p, Customer c " + 
		"WHERE p.pid = c.cid " + 
		"AND c.cid = ?";
	private PreparedStatement maxRentalStatement;
	
	/* BELOW THREE QUERIES ARE USED IN FAST SEARCH */
	// search by movie title
	// (1) movie  (id, m.name, year)
	// (2) director (mid, d.id, d.fname, d.lname)
	// (3) actors (mid, a.id, a.fname, a.lname)
	private static final String MOVIE_ORDER_SQL = 
		"SELECT id, name, year " +
		"FROM Movie " +
		"WHERE name LIKE ? " + 
		"ORDER BY id";
	private PreparedStatement movieOrderStatement;
	
	private static final String DIRECTOR_ORDER_SQL = 
		"SELECT m.id AS mid, d.id, d.fname, d.lname " + 
		"FROM Movie m, Movie_Directors md, Directors d " +
		"WHERE m.id = md.mid " + 
		"AND d.id = md.did " + 
		"AND m.name LIKE ? " + 
		"ORDER BY m.id";
	private PreparedStatement directorOrderStatement;
	
	private static final String ACTOR_ORDER_SQL = 
		"SELECT m.id AS mid, a.id, a.fname, a.lname " + 
		"FROM Actor a, Casts c, Movie m " +
		"WHERE a.id = c.pid " +
		"AND m.id = c.mid " +
		"AND m.name LIKE ? " + 
		"ORDER BY m.id";
	private PreparedStatement actorOrderStatement;
	

	private static final String UPDATE_PLAN_SQL = 
		"UPDATE Customer " + 
		"SET pid = ? " + 
		"WHERE cid = ?";
	private PreparedStatement updatePlanStatement;

	private static final String LIST_PLAN_SQL = 
		"SELECT * FROM Plans";
	private PreparedStatement listPlanStatement;

	private static String RENT_SQL = 
		"INSERT INTO Rental VALUES(?, ?, 'open', CURRENT_TIMESTAMP)";
	private PreparedStatement rentStatement;

	private static final String RETURN_SQL = 
		"UPDATE Rental " +
		"SET status = 'closed' " + 
		"WHERE mid = ? " +
		"AND cid = ?";
	private PreparedStatement returnStatement;
	
	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

    /**********************************************************/
    /* Connection code to SQL Azure. Example code below will connect to
	   the imdb database on Azure
       IMPORTANT NOTE:  You will need to create (and connect to) your new 
       customer database before uncommenting and running the query statements in this file .
     */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("videostore.jdbc_driver");
		jSQLUrl      = configProps.getProperty("videostore.imdb_url");
		jSQLUser     = configProps.getProperty("videostore.sqlazure_username");
		jSQLPassword = configProps.getProperty("videostore.sqlazure_password");
		jSQLCustomerUrl = configProps.getProperty("videostore.customer_url");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the imdb database */
		conn = DriverManager.getConnection(jSQLUrl, 		// database
										   jSQLUser, 		// user
										   jSQLPassword); 	// password
                
		conn.setAutoCommit(true); // by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  */
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
	    customerConn = DriverManager.getConnection(jSQLCustomerUrl, 
												   jSQLUser, 
												   jSQLPassword);
	    customerConn.setAutoCommit(true); //by default,commit after each statement
	    customerConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); 
	}

	public void closeConnection() throws Exception {
		conn.close();
		customerConn.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);

		customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
		beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);

		/* add here more prepare statements for all the other queries you need */
		actorMnameStatement = conn.prepareStatement(ACTOR_SQL);
		currentRentalStatement = customerConn.prepareStatement(CURRENT_RENTAL);
		
		remainingRentalStatement = customerConn.prepareStatement(REMAINING_RENTAL_SQL);
		customerNameStatement = customerConn.prepareStatement(CUSTOMER_NAME_SQL);
		validPlanStatement = customerConn.prepareStatement(VALID_PLAN_SQL);
		validMovieStatement = customerConn.prepareStatement(VALID_MOVIE_SQL);
		maxRentalStatement = customerConn.prepareStatement(MAX_RENTAL);
		
		movieOrderStatement = conn.prepareStatement(MOVIE_ORDER_SQL);
		directorOrderStatement = conn.prepareStatement(DIRECTOR_ORDER_SQL);
		actorOrderStatement = conn.prepareStatement(ACTOR_ORDER_SQL);
		
		updatePlanStatement = customerConn.prepareStatement(UPDATE_PLAN_SQL);
		listPlanStatement = customerConn.prepareStatement(LIST_PLAN_SQL);
		rentStatement = customerConn.prepareStatement(RENT_SQL);
		returnStatement = customerConn.prepareStatement(RETURN_SQL);
		
		searchStatementInjectionFree = conn.prepareStatement(SEARCH_SQL_INJECTION_FREE);
	}


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */
	   
	/* How many movies can she/he still rent?
	   You have to compute and return the difference between the customer's plan
	   and the count of outstanding rentals */
	public int getRemainingRentals(int cid) throws Exception {
		remainingRentalStatement.clearParameters();
		remainingRentalStatement.setInt(1, cid);
		ResultSet remainSet = remainingRentalStatement.executeQuery();
		int remaining;
		if (remainSet.next()) {
			remaining = remainSet.getInt("available");
		} else {
			remaining = 0;
		}
		remainSet.close();
		return remaining;
	}
	
	/* Find the first and last name of the current customer. */
	public String getCustomerName(int cid) throws Exception {
		customerNameStatement.clearParameters();
		customerNameStatement.setInt(1, cid);
		ResultSet nameSet = customerNameStatement.executeQuery();
		String name = "Unkown Name"; 
		if (nameSet.next()) {
			name = nameSet.getString("firstName") + " " + nameSet.getString("lastName");
		}
		nameSet.close();
		return name;
	}

	/* Is planid a valid plan ID?  You have to figure it out */
	public boolean isValidPlan(int planid) throws Exception {
		validPlanStatement.clearParameters();
		validPlanStatement.setInt(1, planid);
		ResultSet planSet = validPlanStatement.executeQuery();
		boolean isValidPlan = false;
		if (planSet.next()) {
			isValidPlan = (planSet.getInt("planCount") > 0);
		} 
		planSet.close();
		return isValidPlan;
	}

	/* is mid a valid movie ID?  You have to figure it out */
	public boolean isValidMovie(int mid) throws Exception {
		validMovieStatement.clearParameters();
		validMovieStatement.setInt(1, mid);
		ResultSet movieSet = validMovieStatement.executeQuery();
		boolean isValidMovie = false;
		if (movieSet.next()) {
			isValidMovie = (movieSet.getInt("movieCount") > 0);
		} 
		movieSet.close();
		return isValidMovie;
	}
	
	// Returns maximum number of movies that the customer can rent.
	// Customer is specified by the given cid.
	private int getMaxRental(int cid) throws Exception {
		maxRentalStatement.clearParameters();
		maxRentalStatement.setInt(1, cid);
		ResultSet maxSet = maxRentalStatement.executeQuery();
		int max = 0;
		if (maxSet.next()) {
			max = maxSet.getInt("maxRental");
		} 
		maxSet.close();
		return max;
	}

	// Find the customer id (cid) of whoever currently rents the movie mid; 
	// return -1 if none
	private int getRenterID(int mid) throws Exception {
		currentRentalStatement.clearParameters();
		currentRentalStatement.setInt(1, mid);
		ResultSet rentalSet = currentRentalStatement.executeQuery();
		int id = -1; // no one rented it
		if (rentalSet.next()) { // someone has it 
			id = rentalSet.getInt("cid");
		} 
		rentalSet.close();
		return id;
	}

    /**********************************************************/
    // login transaction: invoked only once, when the app is started
	// authenticates the user, and returns the user id, or -1 if authentication fails
	public int transaction_login(String name, String password) throws Exception {
		int cid;

		customerLoginStatement.clearParameters();
		customerLoginStatement.setString(1,name);
		customerLoginStatement.setString(2,password);
		ResultSet cid_set = customerLoginStatement.executeQuery();
		if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();
		return(cid);
	}

	/* println the customer's personal data: name, and plan number */
	public void transaction_printPersonalData(int cid) throws Exception {
		String name = getCustomerName(cid);
		int remain = getRemainingRentals(cid);
		int maxRental = getMaxRental(cid);
		System.out.println("Customer Name: " + name);
		System.out.println("You can rent at most " + maxRental + " movies");
		System.out.println("You can rent " + remain + " more movies");
	}


    /**********************************************************/
    /* main functions in this project: */

	// searches for movies with matching titles: 
	// 		SELECT * FROM movie WHERE name LIKE movie_title
	// prints the movies, directors, actors, and the availability status:
	//		AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT
	public void transaction_search(int cid, String movie_title)
			throws Exception {
		/* Interpolate the movie title into the SQL string */
		//String searchSql = SEARCH_SQL_BEGIN + movie_title + SEARCH_SQL_END;
		searchStatementInjectionFree.clearParameters();
		searchStatementInjectionFree.setString(1, "%" + movie_title + "%");
		
		ResultSet movie_set = searchStatementInjectionFree.executeQuery();
		while (movie_set.next()) {
			int mid = movie_set.getInt(1);
			System.out.println("ID: " + mid + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3));
			/* do a dependent join with directors */
			directorMidStatement.clearParameters();
			directorMidStatement.setInt(1, mid);
			ResultSet director_set = directorMidStatement.executeQuery();
			while (director_set.next()) {
				System.out.println("\t\tDirector: " + director_set.getString(3)
						+ " " + director_set.getString(2));
			}
			director_set.close();
			/* now you need to retrieve the actors, in the same manner */
			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			actorMnameStatement.clearParameters();
			actorMnameStatement.setInt(1,mid);
			ResultSet actorSet = actorMnameStatement.executeQuery();
			
			while (actorSet.next()) {
				System.out.println("\t\tActor: " +
						actorSet.getString("fname") + " " + 
						actorSet.getString("lname"));
			}
			actorSet.close();
			
			// RENT AVAILABILITY -> -1 means available, other mid means rented
			int rentId = getRenterID(mid);
			showCurrentOwner(rentId, cid);
		}
		movie_set.close();
		System.out.println();
	}

	private void showCurrentOwner(int rentId, int cid) {
		if (rentId == cid) {
			System.out.println("Search your room");
		} else if (rentId == -1) {
			System.out.println("Movie is available");
		} else {
			System.out.println("Movie's rented by someone");
		}
	}
	
	public void transaction_choosePlan(int cid, int pid) throws Exception {
		try {
			beginTransaction();
			updatePlanStatement.clearParameters();
			updatePlanStatement.setInt(1, pid);
			updatePlanStatement.setInt(2, cid);
			updatePlanStatement.executeUpdate();
			
			if (getRemainingRentals(cid) < 0) {
				rollbackTransaction();
				System.out.println("Update failed: Too many movies rented at this point");
			}
		} catch (Exception e) {
			rollbackTransaction();
		}
	}

	/* println all available plans: SELECT * FROM plan */
	// Only reading, no updating involved -> no need to deal with transaction
	public void transaction_listPlans() throws Exception {
		ResultSet listSet = listPlanStatement.executeQuery();
		System.out.println("----- MOVIE RENTAL PLANS -----");
		while (listSet.next()) {
			int pid = listSet.getInt(1);
			String name = listSet.getString(2);
			int maxRental = listSet.getInt(3);
			int fee = listSet.getInt(4);
			System.out.println("ID: " + pid + 
					"NAME: " + name + 
					"MAX_RENTAL: " + maxRental + 
					"MONTHLY_FEE " + fee);
		}
		listSet.close();
	}
	
	/* rent the movie mid to the customer cid */
	public void transaction_rent(int cid, int mid) throws Exception {
		try {
			beginTransaction();
			int rentId = getRenterID(mid);
			if (getRemainingRentals(cid) < 1 || rentId != -1) {
				rollbackTransaction();
				System.out.println("Cannot rent more movies");
			}

			rentStatement.clearParameters();
			rentStatement.setInt(1, mid);
			rentStatement.setInt(2, cid);
			rentStatement.executeUpdate();
			commitTransaction();
			if (rentId != -1) {
				rollbackTransaction();
				showCurrentOwner(rentId, cid);
			}
		} catch (Exception e) {
			rollbackTransaction();
			System.out.println("Unexpected error occurred during renting");
		}
	}

	/* return the movie mid by the customer cid */
	public void transaction_return(int cid, int mid) throws Exception {
		try {
			beginTransaction();
			int rentId = getRenterID(mid);
			if (rentId != cid) {
				rollbackTransaction();
				System.out.println("I'M CALLING COPS");
			} else {
				returnStatement.clearParameters();
				returnStatement.setInt(1, mid);
				returnStatement.setInt(2, cid);
				returnStatement.executeQuery();
				commitTransaction();
			}
		} catch (Exception e) {
			rollbackTransaction();
		}
	}

	/* like transaction_search, but uses joins instead of dependent joins
	   Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
	   Answers are sorted by mid.
	   Then merge-joins the three answer sets */
	public void transaction_fastSearch(int cid, String movie_title)
			throws Exception {

		movieOrderStatement.clearParameters();
		directorOrderStatement.clearParameters();
		actorOrderStatement.clearParameters();
		
		String formattedInput = "%" + movie_title + "%";
		movieOrderStatement.setString(1, formattedInput);
		directorOrderStatement.setString(1, formattedInput);
		actorOrderStatement.setString(1, formattedInput);
		
		ResultSet movieSet = movieOrderStatement.executeQuery();
		ResultSet directorSet = directorOrderStatement.executeQuery();
		ResultSet actorSet = actorOrderStatement.executeQuery();
		
		// Query results
		// (1) movie  (m.id, m.name, m.year)
		// (2) director (mid, d.id, d.fname, d.lname)
		// (3) actors (mid, a.id, a.fname, a.lname)
		boolean dirNext = directorSet.next();
		boolean actNext = actorSet.next();
		// Merge join - sort mid 
		while (movieSet.next()) {
			int mid = movieSet.getInt("id");
			String title = movieSet.getString("name");
			String year = movieSet.getString("year");
			System.out.println("ID: " + mid + 
					" NAME: " + title + 
					" YEAR: " + year);
					
			// Advance cursor to currently selected movie 
			while (dirNext && mid != directorSet.getInt("mid")) {
				dirNext = directorSet.next();
			}
			while (actNext && mid != actorSet.getInt("mid")) {
				actNext = actorSet.next();
			}
			
			// print info until either there's no more tuples to be processed
			// or mid mismatches
			while (dirNext && mid == directorSet.getInt("mid")) {
				System.out.println("\t\tDirector: " + 
						directorSet.getString("fname") + " " + 
						directorSet.getString("lname"));
				dirNext = directorSet.next();
			}
			while (actNext && mid == actorSet.getInt("mid")) {
				System.out.println("\t\tActor: " + 
						actorSet.getString("fname") + " " +
						actorSet.getString("lname"));
				actNext = actorSet.next();
			}
		}
		movieSet.close();
		directorSet.close();
		actorSet.close();
	}

    // Uncomment helpers below once you've got beginTransactionStatement,
    // commitTransactionStatement, and rollbackTransactionStatement setup from
    // prepareStatements():
    public void beginTransaction() throws Exception {
	    customerConn.setAutoCommit(false);
	    beginTransactionStatement.executeUpdate();	
    }

    public void commitTransaction() throws Exception {
	    commitTransactionStatement.executeUpdate();	
	    customerConn.setAutoCommit(true);
	}
    
	public void rollbackTransaction() throws Exception {
	    rollbackTransactionStatement.executeUpdate();
	    customerConn.setAutoCommit(true);
	} 
}
