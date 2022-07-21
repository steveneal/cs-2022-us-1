package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalVolumeTradedByInstrumentExtractor implements  RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        trades.createOrReplaceTempView("trade");

        String queryResultsToday = String.format("SELECT SUM(LastQty) from trade where EntityID='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), todayMs);

        String queryResultsPastWeek = String.format("SELECT SUM(LastQty) from trade where EntityID='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), pastWeekMs);

        String queryResultsPastYear = String.format("SELECT SUM(LastQty) from trade where EntityID='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), pastYearMs);

        Dataset<Row> volumeToday = session.sql(queryResultsToday);

        Dataset<Row> volumePastWeek = session.sql(queryResultsPastWeek);

        Dataset<Row> volumePastYear = session.sql(queryResultsPastYear);

        Object totalVolumeToday = volumeToday.first().get(0);
        if (totalVolumeToday == null) {
            totalVolumeToday = 0L;
        }

        Object totalVolumePastWeek = volumePastWeek.first().get(0);
        if (totalVolumePastWeek == null){
            totalVolumePastWeek = 0L;
        }

        Object totalVolumePastYear = volumePastYear.first().get(0);
        if (totalVolumePastYear == null){
            totalVolumePastYear = 0L;
        }


        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.instrumentalVolumeToday, totalVolumeToday);
        results.put(RfqMetadataFieldNames.instrumentalVolumePastWeek, totalVolumePastWeek);
        results.put(RfqMetadataFieldNames.instrumentalVolumePastYear, totalVolumePastYear);

        return results;
    }

}
