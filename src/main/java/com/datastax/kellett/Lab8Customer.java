package com.datastax.kellett;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "Lab8", name = "customers",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)

public class Lab8Customer {

    private String country;
    private Integer membership_tier;
    private Integer income_tier;
    private Integer acct_no;
    private String first_name;
    private String last_name;

    // 0-argument constructor
    public Lab8Customer() {}

    // Full constructor
    public Lab8Customer(String country,
                        Integer membership_tier,
                        Integer income_tier,
                        Integer acct_no,
                        String first_name,
                        String last_name) {

        setCountry(country);
        setMembership_tier(membership_tier);
        setIncome_tier(income_tier);
        setAcct_no(acct_no);
        setFirst_name(first_name);
        setLast_name(last_name);
    }

    /*
    Getters
     */

    @PartitionKey(0)
    public String getCountry() {
        return country;
    }

    @PartitionKey(1)
    public Integer getMembership_tier() {
        return membership_tier;
    }

    @ClusteringColumn(0)
    public Integer getIncome_tier() {
        return income_tier;
    }

    @ClusteringColumn(1)
    public Integer getAcct_no() {
        return acct_no;
    }

    @Column
    public String getFirst_name() {
        return first_name;
    }

    @Column
    public String getLast_name() {
        return last_name;
    }

    /*
    Setters - return value instead of void to enable fluent API
     */

    public Lab8Customer setCountry(String country) {
        this.country = country;
        return this;
    }

    public Lab8Customer setMembership_tier(Integer membership_tier) {
        this.membership_tier = membership_tier;
        return this;
    }

    public Lab8Customer setIncome_tier(Integer income_tier) {
        this.income_tier = income_tier;
        return this;
    }

    public Lab8Customer setAcct_no(Integer acct_no) {
        this.acct_no = acct_no;
        return this;
    }

    public Lab8Customer setFirst_name(String first_name) {
        this.first_name = first_name;
        return this;
    }

    public Lab8Customer setLast_name(String last_name) {
        this.last_name = last_name;
        return this;
    }
}
