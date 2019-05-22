package com.datastax.kellett;

/*
In Lab 1 we examine the capabilities of the DSE Driver.
 */

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.lang.System.exit;

public class Lab1 {

    /*
    Add a contact point to the cluster using builder.addContactPoint().
    Note that there is a similar method, addContactPoints, that accepts an array of Strings.
    */
    static void createContactPoint(DseCluster.Builder pBuilder, String pIpAddress) {

        System.out.println("Adding new Contact Point " + pIpAddress + "...");
        pBuilder.addContactPoint(pIpAddress);
        System.out.println("... Contact Point added: " +pIpAddress);

    }

    public static void main(String args[]) {

        System.out.println("Starting Lab 1");

        DseCluster.Builder clusterBuilder = DseCluster.builder();

        /*
        Let's begin by getting one or more contact points to our cluster.
        Normally this would be hard-coded or retrieved from a parameter file,
        but for our lab we'll get them from console input.
        */
        try {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Cluster contact point required.  Enter the IP Address of a cluster node:");
            // Reading data using readLine
            String ipAddress = reader.readLine();

            // Add the first contact point
            createContactPoint(clusterBuilder, ipAddress);

            // Optionally accept more contact points
            do {
                System.out.println("If you want an additional contact point, enter an IP address, otherwise enter n.");
                // Reading data using readLine
                ipAddress = reader.readLine();

                // Add additional contact points
                if (!ipAddress.equalsIgnoreCase("n")) {
                    createContactPoint(clusterBuilder, ipAddress);
                }
            }
            while (!ipAddress.equalsIgnoreCase("n"));

        }

        catch(IOException e) {
            System.out.println(e);
        }

        /*
        Now let's add an optional reconnection policy.  This tells the driver how to attempt to reconnect
        to a downed node.

        The policy we use here is actually the default policy.  It waits exponentially longer between each
        reconnection attempt, but keeps a constant delay once a maximum delay is reached.
         */
        System.out.println("Adding Reconnection Policy...");

        clusterBuilder.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 1600));

        System.out.println("... Reconnection Policy added.");

        /*
        Now let's add an optional default retry policy  We use a policy of INSTANCE here, which means a maximum of
        one retry, and only if enough replicas responded to the original request, but no data was retrieved among them.
         */
        System.out.println("Adding Retry Policy...");

        clusterBuilder.withRetryPolicy(
                DefaultRetryPolicy.INSTANCE
        );

        System.out.println("... Retry Policy added.");

        /*
        Now let's add an optional load balancing policy.  Note that multiple policies can be nested, as we show below.
        Here we use a wrapper TokenAwarePolicy, which contains the child policy DCAwareRounbRobinPolicy.withLocalDc.

        With this configuration, the driver will, on each query, try to use a coordinator node that contains a
        replica of the desired data.  Furthermore, it will try to round-robin requests across all the different nodes
        that hold replicas, and it tries to do all this within the client's local data center.
         */
        System.out.println("Adding Load Balancing Policy...");

        clusterBuilder.withLoadBalancingPolicy(
                new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("DC1").build()
                )
        );

        System.out.println("... Load Balancing Policy added.");

        /*
        Now let's build the connection.  Note that we could have done all of the above with a longer fluent API
        statement, but we wanted to highlight each step.  We'll see the fluent API in our next lab.
         */
        System.out.println("Building connection...");

        DseCluster myCluster = clusterBuilder.build();

        System.out.println("... connection built.");

        /*
        Now we can actually connect to the cluster and create a session.
        The session connection optionally accepts a keyspace.  We'll use dse_perf, one of the standard keyspaces that
        come with DSE.
         */
        System.out.println("Creating a session...");

        DseSession mySession = myCluster.connect("dse_analytics;");

        System.out.println("... Session created.");

        /*
        Let's do a simple read to make sure we really are connected
         */
        ResultSet rs = mySession.execute("select * from dse_analytics.alwayson_sql_info;");
        System.out.println("About to print one row...");

        System.out.println(rs.one().toString());

        System.out.println("... one row printed.");

        /*
        Now we can close the session and terminate.
        */
        System.out.println("Closing session...");

        mySession.close();

        System.out.println("... Session closed.");
        System.out.println("Terminating execution of Lab 1.");
        exit(0);

    }
}
