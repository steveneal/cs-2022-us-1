package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {

    private java.sql.Date since;

    public InstrumentLiquidityExtractor() {
        this.since = new java.sql.Date(new DateTime().minusMonths(1).getMillis());
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");
        String query = String.format("SELECT SUM(LastQty) from trade where EntityID='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), since);

        Dataset<Row> sqlQueryResults = session.sql(query);

        Object totalLastQty = sqlQueryResults.first().get(0);
        if (totalLastQty == null) {
            totalLastQty = 0L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentLiquidity, totalLastQty);

        return results;
    }

    protected void setSince(java.sql.Date since) {
        this.since = since;
    }

}
