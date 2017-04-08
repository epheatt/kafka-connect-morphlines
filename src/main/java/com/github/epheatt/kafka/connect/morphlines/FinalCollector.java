package com.github.epheatt.kafka.connect.morphlines;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

public class FinalCollector implements Command {
    private static final Logger log = LoggerFactory.getLogger(FinalCollector.class);

    private final List<Record> results = new ArrayList<Record>();
    private final Config configs;

    public FinalCollector(Config override) {
        configs = override;
    }

    public List<Record> getRecords() {
        return results;
    }

    public void reset() {
        results.clear();
    }

    public Config getConfigs() {
        return configs;
    }
    
    @Override
    public Command getParent() {
        return null;
    }

    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
        Preconditions.checkNotNull(record);
        results.add(record);
        return true;
    }

}