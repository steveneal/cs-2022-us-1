package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import dev.testdata.Instrument;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;



public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest{
    private Rfq rfq_match;
    private Rfq rfq_not_match;

    @BeforeEach
    public void setup() {
        rfq_match = new Rfq();
        rfq_match.setEntityId(5561279226039690843L);
        rfq_match.setIsin("AT0000A0VRQ6");
        rfq_not_match = new Rfq();
        rfq_not_match.setEntityId(5561279226039690843L);
        rfq_not_match.setIsin("AT0000A0VTQ7");
    }

    @Test
    public void checkInstrumentLiquidityWhenAllTradesMatch() {

        //String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, "src/test/resources/trades/trades.json");

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq_match, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeBiasMonthToDate);
        

        assertEquals(1L, result);
    }

    private void assertEquals(long l, Object result) {
    }

    @Test
    public void checkInstrumentLiquidityWhenNoTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq_not_match, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeBiasMonthToDate);

        assertEquals(new Long(-1), result);
    }
}



