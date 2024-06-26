package org.galatea.service;

import org.galatea.kafka.Trade;
import org.galatea.persistence.repository.TradeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeServiceImpl implements TradeService {

    @Autowired
    private TradeRepository tradeRepository;


    @Override
    public Trade saveTrade(Trade trade) {
        return tradeRepository.save(trade);
    }

    @Override
    public List<Trade> fetchAllTrades() {
        return (List<Trade>) tradeRepository.findAll();
    }
}
