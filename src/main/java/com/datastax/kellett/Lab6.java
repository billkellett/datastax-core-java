package com.datastax.kellett;

/*
In Lab 6 we examine Query pagination basics.
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

public class Lab6 {

    public static void main(String args[]) throws InterruptedException {

        System.out.println("Starting Lab 6");

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

        mySession.execute("DROP KEYSPACE IF EXISTS lab6;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab6 "
                        + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab6.customers_tiered ( acct_no int, first_name text, last_name text, tier int, PRIMARY KEY (tier, acct_no));"
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
                "INSERT INTO lab6.customers_tiered (acct_no, first_name, last_name, tier) VALUES (?, ?, ?, ?)");

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
                BoundStatement cqlInsertBound = cqlInsert.bind(
                        Integer.parseInt(customer[0]),
                        customer[1],
                        customer[2],
                        Integer.parseInt(customer[3]));
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
        System.out.println("Elapsed time: " + elapsedTime / 1000000 + " milliseconds");

        // Sleep 3 seconds to make sure of consistency
        Thread.sleep(3000);

        // set true when last page is retrieved
        boolean lastPage = false;

        // Now read the first page of results from a table
        Lab6PageReader pageReader = new Lab6PageReader();
        ResultSet rsPage = pageReader.readPage(mySession, "");

        /*
        Save the current paging state so you can restore it later.
        NOTE: you MUST save paging state before iterating the result set.
        After iteration, the paging state will no longer exist.
        */
        PagingState pagingState = rsPage.getExecutionInfo().getPagingState();
        String mySavedPagingState = pagingState.toString();
        if(rsPage.isFullyFetched()) {
            lastPage = true;
        }

        System.out.println((" "));
        System.out.println(("Printing Page 1 of results"));
        System.out.println((" "));

        int remaining = rsPage.getAvailableWithoutFetching();

        for (Row row : rsPage) {
            if (remaining > 0) {
                System.out.println(
                        row.getInt("acct_no")
                                + " "
                                + row.getString("first_name")
                                + " "
                                + row.getString("last_name")
                                + " "
                                + row.getInt("tier"));
                remaining--;
            }
        }


        /*
        Now read additional pages of results from the table
        NOTE that we use a new instance of Lab6PageReader in order to show how we transfer paging state
        around the app.  This is a crude simulation of how a web page might pass paging state
        */

        String getAnotherPage = "y";
        int pageNumber = 1;

        while(getAnotherPage.equalsIgnoreCase("y") && !lastPage) {
            try {
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(System.in));

                System.out.println(" ");
                System.out.println("Enter y for next page, any other value to quit:");
                // Reading data using readLine
                getAnotherPage = reader.readLine();
            } catch (IOException e) {
                System.out.println(e);
            }

            if (getAnotherPage.equalsIgnoreCase("y")) {

                Lab6PageReader pageReaderSubsequentPage = new Lab6PageReader();
                rsPage = pageReaderSubsequentPage.readPage(mySession, mySavedPagingState);
                if (rsPage.isFullyFetched()) {
                    lastPage = true;
                }

                /*
                Save the current paging state so you can restore it later.
                NOTE: you MUST save paging state before iterating the result set.
                After iteration, the paging state will no longer exist.
                */
                if(!rsPage.isFullyFetched()) {
                    pagingState = rsPage.getExecutionInfo().getPagingState();
                    mySavedPagingState = pagingState.toString();
                }

                pageNumber++;
                System.out.println((" "));
                System.out.println(("Printing Page " + pageNumber + " of results"));
                System.out.println((" "));

                remaining = rsPage.getAvailableWithoutFetching();

                for (Row row : rsPage) {
                    if (remaining > 0) {
                        System.out.println(
                                row.getInt("acct_no")
                                        + " "
                                        + row.getString("first_name")
                                        + " "
                                        + row.getString("last_name")
                                        + " "
                                        + row.getInt("tier"));
                        remaining--;
                    }
                }
            }
        }

        /*
        Now we can close the session and terminate.
        */
        System.out.println(" ");
        System.out.println("Data retrieval complete.");
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 6.");

        exit(0);
    }
}

