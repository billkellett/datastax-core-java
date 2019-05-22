package com.datastax.kellett;

/*
In Lab 5 we examine Query topics including basic queries, iteration, and QueryBuilder.
*/

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.util.Date;


import static java.lang.System.exit;

public class Lab5 {

    public static void main(String args[]) throws InterruptedException {

        System.out.println("Starting Lab 5");

        String ipAddress = "";
        String csvFile = "";
        String single_acct_no = "";

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
        }

        catch(IOException e) {
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

        mySession.execute("DROP KEYSPACE IF EXISTS lab5;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab5 "
                        + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab5.customers ( acct_no int, first_name text, last_name text, PRIMARY KEY (acct_no));"
        );

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Table created.");

        /*
        Now we'll read a CSV file and write a row to C* for each line in the CSV.
        Using a Batch Statement will reduce network time.
        */
        System.out.println("");
        System.out.println("About to read csv and write to database...");

        //BufferedReader br = null;
        String line = "";
        String csvSplitBy = ",";

        PreparedStatement cqlInsert = mySession.prepare(
                "INSERT INTO lab5.customers (acct_no, first_name, last_name) VALUES (?, ?, ?)");

        BatchStatement cqlInsertBatch = new BatchStatement();

        long startTime = System.nanoTime();
        Date startDateTime = new Date();
        System.out.println("Start time: " + startDateTime);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] customer = line.split(csvSplitBy);

                System.out.print("\rWriting customer " + customer[0]);

                // Create the bound statement and set options
                BoundStatement cqlInsertBound = cqlInsert.bind(Integer.parseInt(customer[0]), customer[1], customer[2]);
                cqlInsertBound.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

                // Add the bound statement to the batch
                cqlInsertBatch.add(cqlInsertBound);
                cqlInsertBatch.enableTracing();

            }

            // execute the batch statement
            mySession.execute(cqlInsertBatch);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("... finished reading csv and writing to database.");
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        Date endDateTime = new Date();
        System.out.println("");
        System.out.println("End time: " + endDateTime);
        System.out.println("Elapsed time: " + elapsedTime/1000000 + " milliseconds");

        /*
        Now that we have some data, let's do a very basic query.

        We'll get an account number from the console, then read that row
        */
        System.out.println(" ");
        System.out.println("Reading 1 row...");

        try {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Enter an account number from 1 to 1000:");
            // Reading data using readLine
            single_acct_no = reader.readLine();
        }
        catch(IOException e) {
            System.out.println(e);
        }

        // Sleep 1 second to make sure of consistency
        Thread.sleep(1000);

        SimpleStatement qry_1 = new SimpleStatement(
                "SELECT acct_no, first_name, last_name from lab5.customers WHERE acct_no = ?",
                    Integer.parseInt(single_acct_no));
        qry_1.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        rs = mySession.execute(qry_1);

        Row oneRow = rs.one();
        System.out.println("Account Number: " + oneRow.getInt("acct_no"));
        System.out.println("First Name: " + oneRow.getString("first_name"));
        System.out.println("Last Name: " + oneRow.getString("last_name"));

        /*
        Next let's iterate over a multi-row result set.
         */
        System.out.println(" ");
        System.out.println("Iterating over multiple rows...");

        SimpleStatement qry_2 = new SimpleStatement(
                "SELECT acct_no, first_name, last_name from lab5.customers LIMIT 10");

        qry_2.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        rs = mySession.execute(qry_2);

        rs.forEach(row -> {
            System.out.println(
                    row.getInt("acct_no")
                            + " "
                            + row.getString("first_name")
                            + " "
                            + row.getString("last_name"));
        });

        /*
        Now let's take a quick look at QueryBuilder
        */
        System.out.println(" ");
        System.out.println("QueryBuilder example");
        System.out.println(" ");

        rs = mySession.execute(
                QueryBuilder.select()
                .column("acct_no")
                .column("first_name")
                .column("last_name")
                .from("lab5", "customers")
                .where(QueryBuilder.eq("acct_no", 1))
        );

        oneRow = rs.one();
        System.out.println("Account Number: " + oneRow.getInt("acct_no"));
        System.out.println("First Name: " + oneRow.getString("first_name"));
        System.out.println("Last Name: " + oneRow.getString("last_name"));

        /*
        Now we can close the session and terminate.
        */
        System.out.println(" ");
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 5.");
        exit(0);

    }
}
