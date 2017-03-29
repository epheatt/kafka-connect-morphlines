package com.github.epheatt.kafka.connect.morphlines;

import com.github.epheatt.kafka.connect.morphlines.MorphlineUtils;

import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * Command that silently consumes records without ever emitting any record - think /dev/null.
 */
public final class ToConnectRecordBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toConnectRecord");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
      return new ToConnectRecord(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Implementation that does logging and metrics */
    private static final class ToConnectRecord extends AbstractCommand {
    
    public ToConnectRecord(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
    }

    @Override
    protected boolean doProcess(Record inputRecord) {
        //ConnectRecord outputRecord = MorphlineUtils.toConnectData(inputRecord);
        // pass record to next command in chain:
        return super.doProcess(inputRecord);
    }
    
  }

}