package com.tzulitai.example3;

import com.tzulitai.example3.solution.EventTimeJoinFunctionSolution;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for {@link EventTimeJoinFunction}.
 */
public class EventTimeJoinTest {

    @Test
    public void testJoin() throws Exception {

        // ==================================================================
        //  Test input data
        // ==================================================================

        final Customer customer_0 = createCustomer(0);
        final Customer customer_500 = createCustomer(500);
        final Customer customer_1500 = createCustomer(1500);
        final Customer customer_1600 = createCustomer(1600);
        final Customer customer_2100 = createCustomer(2100);

        final Trade trade_1000 = createTrade(1000);
        final Trade trade_1200 = createTrade(1200);
        final Trade trade_1500 = createTrade(1500);
        final Trade trade_1700 = createTrade(1700);
        final Trade trade_1800 = createTrade(1800);
        final Trade trade_2000 = createTrade(2000);

        // ==================================================================
        //  Test harness setup
        // ==================================================================

        final EventTimeJoinFunction joinFunction = new EventTimeJoinFunction();
        final KeyedCoProcessOperator<Long, Trade, Customer, EnrichedTrade> operator =
                new KeyedCoProcessOperator<>(joinFunction);

        final KeyedTwoInputStreamOperatorTestHarness<Long, Trade, Customer, EnrichedTrade> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        Trade::customerID,
                        Customer::customerID,
                        BasicTypeInfo.LONG_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        // ==================================================================
        //  Verification
        // ==================================================================

        processCustomer(customer_0, testHarness);

        processTrade(trade_1000, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1000, customer_0.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processCustomer(customer_500, testHarness);
        processCustomer(customer_1500, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1000, customer_500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processTrade(trade_1200, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1200, customer_500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processTrade(trade_1500, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1500, customer_1500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processTrade(trade_1700, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1700, customer_1500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processTrade(trade_1800, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1800, customer_1500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processTrade(trade_2000, testHarness);
        assertEquals(
                new EnrichedTrade(trade_2000, customer_1500.customerInfo()),
                checkAndGetNextOutput(testHarness));

        processCustomer(customer_1600, testHarness);
        processCustomer(customer_2100, testHarness);
        assertEquals(
                new EnrichedTrade(trade_1700, customer_1600.customerInfo()),
                checkAndGetNextOutput(testHarness));
        assertEquals(
                new EnrichedTrade(trade_1800, customer_1600.customerInfo()),
                checkAndGetNextOutput(testHarness));
        assertEquals(
                new EnrichedTrade(trade_2000, customer_1600.customerInfo()),
                checkAndGetNextOutput(testHarness));
    }

    // ==================================================================
    //  Utility methods
    // ==================================================================

    private static void processTrade(
            Trade trade,
            KeyedTwoInputStreamOperatorTestHarness<Long, Trade, Customer, EnrichedTrade> testHarness) throws Exception {
        testHarness.processElement1(new StreamRecord<>(trade));
        testHarness.processWatermark1(new Watermark(trade.timestamp()));
    }

    private static void processCustomer(
            Customer customer,
            KeyedTwoInputStreamOperatorTestHarness<Long, Trade, Customer, EnrichedTrade> testHarness) throws Exception {
        testHarness.processElement2(new StreamRecord<>(customer));
        testHarness.processWatermark2(new Watermark(customer.timestamp()));
    }

    private static Customer createCustomer(long eventTimestamp) {
        return new Customer(eventTimestamp, 911108, "Customer @ t=" + eventTimestamp);
    }

    private static Trade createTrade(long eventTimestamp) {
        return new Trade(eventTimestamp, 911108, "Trade @ t=" + eventTimestamp);
    }

    private static EnrichedTrade checkAndGetNextOutput(
            KeyedTwoInputStreamOperatorTestHarness<Long, Trade, Customer, EnrichedTrade> testHarness) throws Exception {
        while (true) {
            Object next = testHarness.getOutput().poll();
            if (next == null) {
                fail("There are no more outputs.");
            }

            if (next instanceof StreamRecord) {
                @SuppressWarnings("unchecked")
                StreamRecord<EnrichedTrade> casted = (StreamRecord<EnrichedTrade>) next;
                return casted.getValue();
            }
        }
    }
}
