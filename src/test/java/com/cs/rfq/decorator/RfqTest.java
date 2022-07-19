package com.cs.rfq.decorator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RfqTest {

    @Test
    public void testJSonFactoryMethod() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'isin': 'AT0000383864', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 40972357234982251, " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        assertAll(
                () -> assertEquals("123ABC", rfq.getId()),
                () -> assertEquals("AT0000383864", rfq.getIsin()),
                () -> assertEquals((Long) 3351266293154445953L, rfq.getTraderId()),
                () -> assertEquals((Long) 5561279226039690843L, rfq.getEntityId()),
                () -> assertEquals(40972357234982251L, rfq.getInstrumentId()),
                () -> assertEquals((Long) 250000L, rfq.getQuantity()),
                () -> assertEquals((Double) 1.58, rfq.getPrice()),
                () -> assertEquals("B", rfq.getSide()),
                () -> assertTrue(rfq.isBuySide()),
                () -> assertFalse(rfq.isSellSide())
        );

    }
}
