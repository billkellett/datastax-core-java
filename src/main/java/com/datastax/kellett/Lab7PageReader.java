package com.datastax.kellett;

import com.datastax.driver.core.*;

public class Lab7PageReader {

    public ResultSet readPage(Session pSession, String pPagingStateSerialized) {

        SimpleStatement qry = new SimpleStatement(
                "SELECT acct_no, first_name, last_name from lab7.customers ORDER BY acct_no");

        // Note that when using Search, allowed consistency levels are ONE and LOCAL_ONE
        qry.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        qry.setFetchSize(200);

        /*
        If a stringified paging state was passed in, that means we are trying to read a page
        other than page 1.
         */
        if (!pPagingStateSerialized.isEmpty()) {
            qry.setPagingState(PagingState.fromString(pPagingStateSerialized));
        }

        ResultSet rs = pSession.execute(qry);

        return rs;
    }
}

