package it.intre.barbuilder;

import it.intre.barbuilder.model.Bar;
import it.intre.barbuilder.model.Quote;
import it.intre.barbuilder.utils.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

public class Builder {

    private final Logger logger = LogManager.getLogger(Builder.class);

    private String symbol;
    private int timeFrame;
    private BigDecimal open, high, low, close, volume;
    private Long startTimestamp, endTimestamp;

    public Builder(String symbol, int timeFrame) {
        this.symbol = symbol;
        this.timeFrame = timeFrame;
    }

    public Bar addQuote(final Quote quote) {
        Bar bar = null;

        if (this.endTimestamp == null) {
            // first bar
            this.init(quote);
        } else if (DateUtils.getMinuteDifference(this.endTimestamp, quote.getTimestamp()) >= this.timeFrame) {
            // bar completed
            this.logger.debug("Building bar for {}...", quote.getSymbol());
            bar = this.buildBar();
            this.logger.debug("Built bar: {}", bar);
            this.init(quote);
        } else {
            // same bar
            this.update(quote);
        }

        return bar;
    }

    private void init(final Quote quote) {
        this.logger.trace("Initializing bar for {}...", quote.getSymbol());
        this.open = quote.getPrice();
        this.high = quote.getPrice();
        this.low = quote.getPrice();
        this.close = quote.getPrice();
        this.volume = quote.getVolume();
        this.startTimestamp = DateUtils.truncateSeconds(quote.getTimestamp());
        this.endTimestamp = this.startTimestamp + this.timeFrame * 60000L;
    }

    private void update(final Quote quote) {
        this.logger.trace("Updating bar for {}...", quote.getSymbol());
        BigDecimal price = quote.getPrice();
        this.high = price.compareTo(this.high) > 0 ? price : this.high;
        this.low = price.compareTo(this.low) < 0 ? price : this.low;
        this.close = price;
        this.volume = this.volume.add(quote.getVolume());
    }

    private Bar buildBar() {
        return new Bar(this.symbol, this.open, this.high, this.low, this.close, this.volume, this.startTimestamp, this.endTimestamp);
    }
}
