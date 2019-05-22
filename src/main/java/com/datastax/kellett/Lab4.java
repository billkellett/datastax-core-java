package com.datastax.kellett;

/*
In Lab 4 we examine Batch Statements.
*/

import com.datastax.driver.core.*;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.util.Date;


import static java.lang.System.exit;

public class Lab4 {

    public static void main(String args[]) throws InterruptedException {

        System.out.println("Starting Lab 4");

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

        mySession.execute("DROP KEYSPACE IF EXISTS lab4;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab4 "
                        + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab4.customers ( acct_no int, first_name text, last_name text, PRIMARY KEY (acct_no));"
        );

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Table created.");

        /*
        We are going to truncate the system tracing tables to make sure that only our new data will be present.
        !!! NEVER NEVER NEVER do this in production !!!
         */
        mySession.execute("TRUNCATE system_traces.sessions;");
        mySession.execute("TRUNCATE system_traces.events;");

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
                "INSERT INTO lab4.customers (acct_no, first_name, last_name) VALUES (?, ?, ?)");

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

        System.out.println("DSE tracing does not capture durations of individual transactions in a batch");

        /*
        Now we can close the session and terminate.
        */
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 4.");
        exit(0);

    }
}

