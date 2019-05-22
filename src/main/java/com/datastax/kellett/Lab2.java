package com.datastax.kellett;

/*
In Lab 2 we examine Simple Statements.
Simple Statements are useful for statements that are only executed once.
We show an ANTI-PATTERN of using Simple Statements multiple times, and show the performance penalty.

There are two reasons to use Simple Statements instead of Session.execute
1. You can use parameter placeholders
2. You can set options (consistency level, tracing, etc.)
*/

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.util.Date;


import static java.lang.System.exit;

public class Lab2 {

    public static void main(String args[]) throws InterruptedException {

        System.out.println("Starting Lab 2");

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

        mySession.execute("DROP KEYSPACE IF EXISTS lab2;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab2 "
                    + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab2.customers ( acct_no int, first_name text, last_name text, PRIMARY KEY (acct_no));"
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
        Using a Simple Statement makes it easier to insert parameters, without worrying about concatenating
        them into a single string.
        However, we want to show here that using a Simple Statement in a loop is non-performant.
        */
        System.out.println("");
        System.out.println("About to read csv and write to database...");

        //BufferedReader br = null;
        String line = "";
        String csvSplitBy = ",";
        long startTime = System.nanoTime();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] customer = line.split(csvSplitBy);

                System.out.print("\rWriting customer " + customer[0]);

                Statement cqlInsert = new SimpleStatement(
                        "INSERT INTO lab2.customers (acct_no, first_name, last_name) VALUES (?, ?, ?)"
                        , Integer.parseInt(customer[0]), customer[1], customer[2]);

                /*
                Set options for the SimpleStatement.
                Note that we enable tracing.  This causes trace information to be written asynchronously to
                system_traces.sessions and system_traces.events.  We won't further impact performance by retrieving
                that data in this program.  Instead, we'll look at it in Studio.  Note that these tables store
                elapsed time information in microseconds.
                */
                cqlInsert.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                cqlInsert.enableTracing();

                // execute the SimpleStatement
                mySession.execute(cqlInsert);
            }

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

        // Now get the average elapsed time server-side per query
        // Pause 10 seconds to make sure system_traces records were written
        System.out.println("");
        System.out.println("Beginning 10-second pause to quiesce system_traces.");
        Thread.sleep(10000);
        System.out.println("10-second pause complete.");
        System.out.println("");

        rs = mySession.execute(
                "select avg(duration) as avg_duration from system_traces.sessions;");
        System.out.println("Average server-side elapsed time per query: " + rs.one().getInt("avg_duration") + " microseconds" );

        rs = mySession.execute(
                "select avg(source_elapsed) as avg_source_elapsed from system_traces.events where activity = 'Preparing statement' allow filtering");
        System.out.println("Average server-side elapsed time spent in query preparation phase: " + rs.one().getInt("avg_source_elapsed") + " microseconds");

        System.out.println("");

        /*
        Now we can close the session and terminate.
        */
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 2.");
        exit(0);

    }
}

