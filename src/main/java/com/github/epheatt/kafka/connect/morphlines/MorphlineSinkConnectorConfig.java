/**
 * Copyright © 2017 Eric Pheatt (eric.pheatt@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.epheatt.kafka.connect.morphlines;

import com.google.common.base.Strings;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MorphlineSinkConnectorConfig extends AbstractConfig {
    public static final String MORPHLINE_FILE_CONFIG = "morphlines.morphlineFile";
    public static final String MORPHLINE_ID_CONFIG = "morphlines.morphlineId";
    private static final String MORPHLINE_FILE_DOC = "Configures Morphline File with the command pipeline.";
    private static final String MORPHLINE_ID_DOC = "Configures specific Morphline to run from within Morphline File.";

    public final String morphlineFile;
    public final String morphlineId;

    protected MorphlineSinkConnectorConfig(Map<String, String> props) {
        super(config(), props);
        this.morphlineFile = this.getString(MORPHLINE_FILE_CONFIG);
        this.morphlineId = this.getString(MORPHLINE_ID_CONFIG);
    }

    public static ConfigDef config() {
        return new ConfigDef().define(MORPHLINE_FILE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, MORPHLINE_FILE_DOC).define(
                MORPHLINE_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, MORPHLINE_ID_DOC);
    }
}
