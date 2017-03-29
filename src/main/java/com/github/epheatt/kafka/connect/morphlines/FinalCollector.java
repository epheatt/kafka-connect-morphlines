package com.github.epheatt.kafka.connect.morphlines;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;

import com.typesafe.config.Config;

public class FinalCollector implements Command {

    private final List<Record> results = new ArrayList();
    private final Config settings;

    public FinalCollector(Config override) {
        settings = override;
    }

    public List<Record> getRecords() {
        return results;
    }

    public void reset() {
        results.clear();
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
        // Preconditions.checkNotNull(record);
        results.add(record);
        return true;
    }

}