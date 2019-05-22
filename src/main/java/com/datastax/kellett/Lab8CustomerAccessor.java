package com.datastax.kellett;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface Lab8CustomerAccessor {

    @Query("SELECT * FROM lab8.customers;")
    Result<Lab8Customer> getAll();

    @Query("SELECT * FROM lab8.customers WHERE country = ? and membership_tier = ?")
    Result<Lab8Customer> getPartition(String country, Integer membership_tier);

    @Query("SELECT * FROM lab8.customers WHERE country = ? and membership_tier = ? and income_tier = ?")
    Result<Lab8Customer> getPartialPartition(String country, Integer membership_tier, Integer income_tier);
}
