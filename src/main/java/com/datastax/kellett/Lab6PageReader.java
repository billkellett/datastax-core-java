package com.datastax.kellett;

import com.datastax.driver.core.*;

public class Lab6PageReader {

    public ResultSet readPage(Session pSession, String pPagingStateSerialized) {

        SimpleStatement qry = new SimpleStatement(
                "SELECT acct_no, first_name, last_name, tier from lab6.customers_tiered WHERE tier = 2");

        qry.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        qry.setFetchSize(20);

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
