package com.datastax.kellett;

/*
In Lab 9 we examine Asynchronous Operations.
*/

import com.datastax.driver.core.*;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.time.LocalTime;
import java.util.Date;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static java.lang.System.exit;

public class Lab9 {

    private static List<ResultSetFuture> sendQueries(Session session, String query, String[] account_numbers) {

        // Start a query for each account number in the list, and return a list of futures.
        List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(account_numbers.length);

        for (Object account_number : account_numbers)
            futures.add(session.executeAsync(new String(query + " " + account_number)));

        return futures;
    }

    private static Future<List<ResultSet>> queryAllAsList( Session session, String query, String[] account_numbers) {

        // List of Futures is available when ALL queries are complete.
        List<ResultSetFuture> futures = sendQueries(session, query, account_numbers);
        return Futures.successfulAsList((futures));
    }

    private static List<ListenableFuture<ResultSet>> queryAllAsAvailable(Session session, String query, String[] account_numbers) {

        // Similar to sendQueries() above, but each future is available as soon as it completes.
        List<ResultSetFuture> futures = sendQueries(session, query, account_numbers);
        return Futures.inCompletionOrder(futures);
    }

    public static void main(String args[]) throws InterruptedException, ExecutionException {

        System.out.println("Starting Lab 9");

        String ipAddress = "";
        String csvFile = "";

        /*
        Let's begin by getting one contact point to our cluster, and the path to our input data.
        */
        try {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Cluster contact point required.  Enter the IP Address of a cluster node:");
            // Reading data using readLine
            ipAddress = reader.readLine();

            System.out.println("Enter path/filename of input csv file:");
            // Reading data using readLine
            csvFile = reader.readLine();
        } catch (IOException e) {
            System.out.println(e);
        }

        /*
        Now let's build the connection.
        */
        DseCluster myCluster = DseCluster.builder().addContactPoint(ipAddress).build();

        /*
        Now we can actually connect to the cluster and create a session.
        */
        DseSession mySession = myCluster.connect();

        /*
        Session.execute() is the simplest way to run CQL.  It's appropriate for 1-time commands like DDL.
        Let's use it to create a Keyspace.
        */
        System.out.println("About to create Keyspace...");

        mySession.execute("DROP KEYSPACE IF EXISTS lab9;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab9 "
                        + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab9.customers ( acct_no int, first_name text, last_name text, PRIMARY KEY (acct_no));"
        );

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Table created.");

        /*
        Now we'll read a CSV file and write a row to C* for each line in the CSV.
        Using a Prepared Statement is more efficient than a Simple Statement.
        */
        System.out.println("");
        System.out.println("About to read csv and write to database...");

        //BufferedReader br = null;
        String line = "";
        String csvSplitBy = ",";

        PreparedStatement cqlInsert = mySession.prepare(
                "INSERT INTO lab9.customers (acct_no, first_name, last_name) VALUES (?, ?, ?)");

        long startTime = System.nanoTime();
        Date startDateTime = new Date();
        System.out.println("Start time: " + startDateTime);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] customer = line.split(csvSplitBy);

                // Create the bound statement and set options
                BoundStatement cqlInsertBound = cqlInsert.bind(Integer.parseInt(customer[0]), customer[1], customer[2]);
                cqlInsertBound.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

                if(Integer.parseInt(customer[0]) < 101) {
                    System.out.println("Beginning write for customer "
                            + customer[0]
                            + " at "
                            + LocalTime.now());

                    ResultSetFuture future = mySession.executeAsync(cqlInsertBound);
                    while (!future.isDone()) {
                        //System.out.print("Doing something else while writing customer "
                         //       + customer[0]);
                        System.out.print(".");
                        //System.out.println(mySession.execute(cqlCountBound).toString());
                        Thread.sleep(10);
                    }
                    System.out.println("");
                    System.out.println("Completed writing customer "
                                        + customer[0]
                                        + " at "
                                        + LocalTime.now());

                    rs = future.get();
                    System.out.println("Insert successful: " + rs.wasApplied());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("");
        System.out.println("... finished reading csv and writing to database.");

        // Pause 1 second to quiesce
        System.out.println("");
        System.out.println("Beginning 1-second pause to ensure consistency.");
        Thread.sleep(1000);
        System.out.println("1-second pause complete.");
        System.out.println("");

        // Now we'll query for a specific 20 of the records we just inserted.
        String[] account_numbers = {"5", "10", "15", "20", "25", "30", "35", "40", "45", "50",
                                    "55", "60", "65", "70", "75", "80", "85", "90", "95", "100"};
        String query = "SELECT * FROM lab9.customers WHERE acct_no =";

        // This will give us the results, in the order we submitted them, when ALL futures are complete.
        System.out.println("");
        System.out.println("Show query results when all async queries are complete.");
        Future<List<ResultSet>> future = queryAllAsList(mySession, query, account_numbers);

        List<ResultSet> rsList = future.get();

        rsList.forEach(result -> {
            result.forEach(row -> {
                System.out.println(
                        row.getInt("acct_no")
                                + " "
                                + row.getString("first_name")
                                + " "
                                + row.getString("last_name"));
            });
        });

        // Now we do a similar query, but we get each result as soon as it is available
        System.out.println("");
        System.out.println("Show query results as soon as they are available.");
        List<ListenableFuture<ResultSet>> futuresAsAvailable = queryAllAsAvailable(mySession, query, account_numbers);

        for (ListenableFuture<ResultSet> futureAvailable : futuresAsAvailable) {
            ResultSet rsAvailable = futureAvailable.get();
            rsAvailable.forEach(row -> {
                System.out.println(
                        row.getInt("acct_no")
                                + " "
                                + row.getString("first_name")
                                + " "
                                + row.getString("last_name"));
            });
        }

        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        Date endDateTime = new Date();
        System.out.println("");
        System.out.println("End time: " + endDateTime);
        System.out.println("Elapsed time: " + elapsedTime / 1000000 + " milliseconds");

        /*
        Now we can close the session and terminate.
        */
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 9.");
        exit(0);

    }
}
