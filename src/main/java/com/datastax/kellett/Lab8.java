package com.datastax.kellett;

/*
In Lab 8 we examine Mapper
*/

import com.datastax.driver.core.*;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.util.Date;
import java.util.Objects;

import static java.lang.System.exit;

public class Lab8 {

    public static void main(String args[]) throws InterruptedException {

        System.out.println("Starting Lab 8");

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

        mySession.execute("DROP KEYSPACE IF EXISTS lab8;");

        ResultSet rs = mySession.execute(
                "CREATE KEYSPACE lab8 "
                        + "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 3 };");

        System.out.println("Operation successful: " + rs.wasApplied());
        System.out.println("... Keyspace created.");

        /*
        Now create a table
        */
        System.out.println("About to create Table...");

        rs = mySession.execute(
                "CREATE TABLE IF NOT EXISTS "
                        + "lab8.customers ( country text, membership_tier int, income_tier int, "
                        + "acct_no int, first_name text, last_name text, "
                        + "PRIMARY KEY ((country, membership_tier), income_tier, acct_no));"
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
                "INSERT INTO lab8.customers (country, membership_tier, income_tier, "
                + "acct_no, first_name, last_name) VALUES (?, ?, ?, ?, ?, ?)");

        BatchStatement cqlInsertBatch = new BatchStatement();

        long startTime = System.nanoTime();
        Date startDateTime = new Date();
        System.out.println("Start time: " + startDateTime);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] customer = line.split(csvSplitBy);

                System.out.print("\rWriting customer " + customer[3]);

                // Create the bound statement and set options
                BoundStatement cqlInsertBound = cqlInsert.bind(
                        customer[0],
                        Integer.parseInt(customer[1]),
                        Integer.parseInt(customer[2]),
                        Integer.parseInt(customer[3]),
                        customer[4],
                        customer[5]);
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

        /*
        Now we'll define a Mapping Manager and see how it works
         */
        MappingManager mapManager = new MappingManager(mySession);

        // Get the mapper for the customers table
        Mapper<Lab8Customer> mapCustomers = mapManager.mapper(Lab8Customer.class);

        // Insert a record
        System.out.println(" ");
        System.out.println("About to insert record...");

        Lab8Customer newCustomer = new Lab8Customer();
        newCustomer.setCountry("US")
                .setMembership_tier(1)
                .setIncome_tier(2)
                .setAcct_no(9999)
                .setFirst_name("Test")
                .setLast_name("Customer");

        mapCustomers.save(newCustomer);

        System.out.println("... record inserted.");

        /*
        We'll insert waits between operations.  It shouldn't be necessary with the current mapper consistency
        settings, but if you change them in the future, you might need the wait.
         */
        Thread.sleep(1000);

        // Read the inserted record
        Lab8Customer retrievedCustomer = mapCustomers.get("US", 1, 2, 9999);

        System.out.println(" ");
        System.out.println("Newly inserted record retrieved from database:");
        System.out.println("Country: " + retrievedCustomer.getCountry());
        System.out.println("Membership Tier: " + retrievedCustomer.getMembership_tier());
        System.out.println("Income Tier: " + retrievedCustomer.getIncome_tier());
        System.out.println("Account Number: " + retrievedCustomer.getAcct_no());
        System.out.println("First Name: " + retrievedCustomer.getFirst_name());
        System.out.println("Last Name: " + retrievedCustomer.getLast_name());

        Thread.sleep(1000);

        // Now we'll update the record
        System.out.println(" ");
        System.out.println("About to update record...");

        retrievedCustomer.setFirst_name("Satisfied");
        mapCustomers.save(retrievedCustomer);

        System.out.println("... record updated.");

        // Read the updated record
        Lab8Customer updatedCustomer = mapCustomers.get("US", 1, 2, 9999);

        System.out.println(" ");
        System.out.println("Updated record retrieved from database:");
        System.out.println("Country: " + updatedCustomer.getCountry());
        System.out.println("Membership Tier: " + updatedCustomer.getMembership_tier());
        System.out.println("Income Tier: " + updatedCustomer.getIncome_tier());
        System.out.println("Account Number: " + updatedCustomer.getAcct_no());
        System.out.println("First Name: " + updatedCustomer.getFirst_name());
        System.out.println("Last Name: " + updatedCustomer.getLast_name());

        Thread.sleep(1000);

        /*
        Now delete the record.  There are two ways to do it:
        1. Supply the primary key
        2. Delete the Java object - we'll do this, since it is simpler than supplying all 4 key fields
         */
        System.out.println(" ");
        System.out.println("About to delete record...");

        mapCustomers.delete(retrievedCustomer);

        System.out.println("... record deleted.");

        Thread.sleep(1000);

        // Now we'll try to read the deleted record.  If the record does not exist, Mapper.get returns null
        if(Objects.isNull(mapCustomers.get("US", 1, 2, 9999))) {
            System.out.println("Tried to read, but record was successfully deleted from database.  ");
        }
        else {
            System.out.println("Unexpected result.  Record was not deleted.");
        }

        /*
        Basic CRUD operations are working.  Now let's think about multi-row result sets
         */

        System.out.println(" ");
        System.out.println("About to read multiple rows using Accessor");
        System.out.println(" ");

        // First tell MappingManager to process the interface and generate an implementation for it
        Lab8CustomerAccessor customerAccessor = mapManager.createAccessor(Lab8CustomerAccessor.class);

        // Now fill in the parameters and execute the query
        Result<Lab8Customer> qryResults = customerAccessor.getPartialPartition(
                                            "US",
                                            1,
                                            2);

        // Now print the results
        for (Lab8Customer row : qryResults) {
            System.out.println(
                    row.getCountry()
                            + " "
                            + row.getMembership_tier()
                            + " "
                            + row.getIncome_tier()
                            + " "
                            + row.getAcct_no()
                            + " "
                            + row.getFirst_name()
                            + " "
                            + row.getLast_name());
        }

        /*
        Now we can close the session and terminate.
        */
        System.out.println(" ");
        System.out.println("Data retrieval complete.");
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 8.");

        exit(0);
    }
}



