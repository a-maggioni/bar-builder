package it.intre.barbuilder;

import it.intre.barbuilder.common.Constants;
import it.intre.barbuilder.model.Bar;
import it.intre.barbuilder.model.Quote;
import it.intre.messagedispatcher.consumer.Consumer;
import it.intre.messagedispatcher.consumer.KafkaConsumer;
import it.intre.messagedispatcher.model.KafkaConfiguration;
import it.intre.messagedispatcher.model.KafkaRecord;
import it.intre.messagedispatcher.model.Record;
import it.intre.messagedispatcher.producer.KafkaProducer;
import it.intre.messagedispatcher.producer.Producer;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws ParseException {
        CommandLine commandLine = getCommandLine(args);
        final String host = commandLine.getOptionValue("host");
        final String port = commandLine.getOptionValue("port");
        final int timeFrame = ((Number) commandLine.getParsedOptionValue("timeFrame")).intValue();

        Map<String, Builder> builderMap = new HashMap<>();
        KafkaConfiguration inputConfiguration = new KafkaConfiguration(host, port, Constants.GROUP_ID, Constants.CLIENT_ID, Constants.INPUT_TOPIC);
        Consumer consumer = new KafkaConsumer<>(inputConfiguration, String.class, Quote.class);
        KafkaConfiguration outputConfiguration = new KafkaConfiguration(host, port, Constants.GROUP_ID, Constants.CLIENT_ID, Constants.OUTPUT_TOPIC);
        Producer producer = new KafkaProducer<String, Bar>(outputConfiguration);

        while (true) {
            List<Record<String, Quote>> quotesRecords = consumer.receive();
            for (Record<String, Quote> quoteRecord : quotesRecords) {
                String symbol = quoteRecord.getKey();
                Quote quote = quoteRecord.getValue();
                if (!builderMap.containsKey(symbol)) {
                    builderMap.put(symbol, new Builder(symbol, timeFrame));
                }
                Builder builder = builderMap.get(symbol);
                Bar bar = builder.addQuote(quote);
                if (bar != null) {
                    Record barRecord = new KafkaRecord<>(Constants.OUTPUT_TOPIC, bar.getSymbol(), bar);
                    boolean success = producer.send(barRecord);
                    if (success) {
                        logger.debug("Sent bar: {}", bar);
                    }
                }
            }
            consumer.commit();
        }
    }

    private static CommandLine getCommandLine(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        HelpFormatter helpFormatter = new HelpFormatter();
        Options options = getOptions();
        CommandLine commandLine = null;
        try {
            commandLine = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            helpFormatter.printHelp("BarBuilder", options);
            System.exit(1);
        }
        return commandLine;
    }

    private static Options getOptions() {
        Options options = new Options();
        Option host = new Option("h", "host", true, "Kafka host");
        host.setRequired(true);
        options.addOption(host);
        Option port = new Option("p", "port", true, "Kafka port");
        port.setRequired(true);
        options.addOption(port);
        Option timeFrame = new Option("tf", "timeFrame", true, "Time frame (minutes)");
        timeFrame.setType(Number.class);
        timeFrame.setRequired(true);
        options.addOption(timeFrame);
        return options;
    }

}
