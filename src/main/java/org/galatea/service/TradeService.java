package org.galatea.service;


import org.galatea.kafka.Trade;

import java.util.List;

public interface TradeService {

    Trade saveTrade(Trade trade);

    List<Trade> fetchAllTrades();
}
