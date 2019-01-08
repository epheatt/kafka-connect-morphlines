package com.github.epheatt.kafka.connect.morphlines;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;

import com.google.common.base.Preconditions;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.errors.RetriableException;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * The Class EnrichJsonBuilder builds an EnrichJson command.
 */
public class EnrichJsonBuilder implements CommandBuilder {

	/* (non-Javadoc)
	 * @see org.kitesdk.morphline.api.CommandBuilder#build(com.typesafe.config.Config, org.kitesdk.morphline.api.Command, org.kitesdk.morphline.api.Command, org.kitesdk.morphline.api.MorphlineContext)
	 */
	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new EnrichJson(this, config, parent, child, context);
	}

	/* (non-Javadoc)
	 * @see org.kitesdk.morphline.api.CommandBuilder#getNames()
	 */
	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("enrichJson");
	}

	/**
	 * The Class EnrichJson is a Morphline command to enrich input json object as per avro schema.
	 */
	private static final class EnrichJson extends AbstractCommand {

		/** The Constant log. */
		private static final Logger log = LoggerFactory.getLogger(EnrichJson.class);
		
		/** The Constant mapper. */
		private static final ObjectMapper mapper = new ObjectMapper();
		
		/** The Constant TIMESTAMP_MILLIS. */
		private static final String TIMESTAMP_MILLIS = "timestamp-millis";
		
		/** The value field stores value field name to fetch from input record. */
		private final String valueField;

		private final String schemaField;

		private final Schema fixedSchema;

		/** The schema registry url. */
		private final String schemaRegistryUrl;
		
		/** The subject field stores subject for which avro schema needs to be fetched. */
		private final String subjectField;
		
		/** The schema registry client. */
		//private final CachedSchemaRegistryClient schemaRegistryClient;

		private static final AvroData AVRO_CONVERTER;
		static {
			AVRO_CONVERTER = new AvroData(new AvroDataConfig.Builder()
					//.with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
					.build());
		}

		/**
		 * Instantiates a new enrich json and do input validations.
		 *
		 * @param builder the builder
		 * @param config the config
		 * @param parent the parent
		 * @param child the child
		 * @param context the context
		 */
		protected EnrichJson(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			this.valueField = getConfigs().getString(config, "valueField", null);
			if(valueField == null) {
				throw new MorphlineCompilationException(
		                  "valueField must be defined", config);
			}
			this.schemaField = getConfigs().getString(config, "schemaField", null);
			String schemaString = getConfigs().getString(config, "schemaString", null);
			this.schemaRegistryUrl = getConfigs().getString(config, "schema-registry-url", null);
			int numDefinitions = 0;
			if (schemaField != null) {
				numDefinitions++;
			}
			if (schemaString != null) {
				numDefinitions++;
			}
			if (schemaRegistryUrl != null) {
				numDefinitions++;
			}
			if (numDefinitions == 0) {
				throw new MorphlineCompilationException(
						"Either schemaString or schemaField or schemaRegistryUrl must be defined", config);
			}
			this.subjectField = getConfigs().getString(config, "subjectField", null);
			if(subjectField == null) {
				throw new MorphlineCompilationException(
		                  "subjectField must be defined", config);
			}
			if (schemaString != null) {
				this.fixedSchema = getSchemaFor(schemaString);
			} else {
				this.fixedSchema = null;
			}
            //if (schemaField == null && schemaString == null && schemaRegistryUrl != null) {
			//	this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
			//}
		}

		/* (non-Javadoc)
		 * @see org.kitesdk.morphline.base.AbstractCommand#doProcess(org.kitesdk.morphline.api.Record)
		 */
		@Override
		protected boolean doProcess(Record inputRecord) {
			Record outputRecord = inputRecord.copy();
		    AbstractParser.removeAttachments(outputRecord);
			String valueField = this.valueField != null ? this.valueField : Fields.ATTACHMENT_BODY;
			Object value = inputRecord.getFirstValue(valueField != null ? valueField : Fields.ATTACHMENT_BODY);
			log.debug("before enrichment value: " + value);
			String subject = (String) inputRecord.getFirstValue(subjectField);
			Schema valueSchema;
			if (schemaField != null) {
				Object schema = inputRecord.getFirstValue(schemaField);
				if(schema instanceof org.apache.kafka.connect.data.Schema)
					valueSchema = AVRO_CONVERTER.fromConnectSchema((org.apache.kafka.connect.data.Schema) schema);
				else
					valueSchema = (Schema) schema;
				Preconditions.checkNotNull(valueSchema);
			} else if (fixedSchema != null) {
				valueSchema = fixedSchema;
			} else {
				valueSchema = fetchAvroSchemaFromSchemaRegistry(subject);
			}
			if(valueSchema != null) {
				log.debug("enrich schema: " + valueSchema);
				Object enrichedValue = enrichJsonValueAsPerAvroSchema(valueSchema, value);
				outputRecord.put(valueField, enrichedValue);
				outputRecord.put("valueSchema", valueSchema);
				log.debug("enriched value: " + enrichedValue);
				return super.doProcess(outputRecord);
			} else {
				log.warn("skipping enrichment as no schema found for subject" + subject);
				return super.doProcess(inputRecord);
			}
		}

		private static Schema getSchemaFor(String str) {
			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(str);
			return schema;
		}

		/**
		 * Fetch avro schema from schema registry.
		 *
		 * @param subject the subject
		 * @return the schema
		 */
		private Schema fetchAvroSchemaFromSchemaRegistry(String subject) {
			CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(this.schemaRegistryUrl, 100);
			try {
				return schemaRegistryClient.getBySubjectAndId(subject,
						schemaRegistryClient.getLatestSchemaMetadata(subject).getId());
			} catch (IOException | RestClientException e) {
				log.error("Error in fetching schema from registry", e);
			}
			return null;
		}

		/**
		 * Enrich json value as per avro schema.
		 *
		 * @param valueSchema the value schema
		 * @param value the value
		 * @return the object
		 */
		private Object enrichJsonValueAsPerAvroSchema(Schema valueSchema, Object value) {
			ObjectNode valueNode = mapper.convertValue(value, ObjectNode.class);
			ObjectNode updatedNode = mapper.createObjectNode();
			for (Schema.Field field : valueSchema.getFields()) {
				updateValueAsPerAvroSchema(field, field.schema(), (ObjectNode) valueNode, updatedNode, Boolean.FALSE);
			}
			return updatedNode;
		}

		/**
		 * Update value as per avro schema.
		 *
		 * @param field the field
		 * @param valueSchema the value schema
		 * @param valueNode the value node
		 * @param updatedNode the updated node
		 * @param isUnionSubtype the is union subtype
		 */
		private void updateValueAsPerAvroSchema(Schema.Field field, Schema valueSchema, ObjectNode valueNode,
				ObjectNode updatedNode, boolean isUnionSubtype) {
			String fieldName = field.name();
			switch (valueSchema.getType()) {
			case UNION:
				for (Schema unionSchema : field.schema().getTypes()) {
					if (unionSchema.getType().equals(Type.NULL)) {
						continue;
					} else {
						updateValueAsPerAvroSchema(field, unionSchema, valueNode, updatedNode, Boolean.TRUE);
					}
				}
				break;
			case ARRAY:
				updateValueAsPerArraySchema(field, valueSchema, valueNode, updatedNode, isUnionSubtype, fieldName);
				break;
			case RECORD:
				if (valueNode.get(fieldName) != null) {
					ObjectNode recordNode = mapper.createObjectNode();
					for (Schema.Field recordField : valueSchema.getFields()) {
						updateValueAsPerAvroSchema(recordField, recordField.schema(),
								(ObjectNode) valueNode.get(fieldName), recordNode, Boolean.FALSE);
					}
					if(isUnionSubtype) {
						ObjectNode recordNamespaceNode = mapper.createObjectNode();
						recordNamespaceNode.set(valueSchema.getFullName(), recordNode);
						updatedNode.set(fieldName, recordNamespaceNode);
					} else {
						updatedNode.set(fieldName, recordNode);
					}
				} else {
					updatedNode.set(fieldName, null);
				}

				break;
			case STRING:
				JsonNode fieldValue = valueNode.get(fieldName);
				if (fieldValue != null) {
					if(isUnionSubtype) {
						ObjectNode innerNode = mapper.createObjectNode();
						innerNode.put(Schema.Type.STRING.getName(), fieldValue.asText());
						updatedNode.set(fieldName, innerNode);	
					} else {
						updatedNode.put(fieldName, fieldValue.asText());
					}
					
				} else {
					updatedNode.set(fieldName, null);
				}

				break;
			case INT:
				JsonNode intValue = valueNode.get(fieldName);
				if (intValue != null) {
					if(isUnionSubtype) {
						ObjectNode innerNode = mapper.createObjectNode();
						innerNode.put(Schema.Type.INT.getName(), intValue.asInt());
						updatedNode.set(fieldName, innerNode);	
					} else {
						updatedNode.put(fieldName, intValue.asInt());
					}
				} else {
					updatedNode.set(fieldName, null);
				}
				break;
			case LONG:
				JsonNode longValue = valueNode.get(field.name());
				if (longValue != null) {
					long longVal;
					if (TIMESTAMP_MILLIS.equals(valueSchema.getProp("logicalType"))) {
					    try {
					        longVal = Instant.parse(longValue.asText()).toEpochMilli();
					    } catch (DateTimeParseException e) {
					    	log.error("Incorrect date string in record : " + longValue);
					        updatedNode.set(fieldName, null);
					        break;
					    }
					} else {
						longVal = longValue.asLong();
					}
					if(isUnionSubtype) {
						ObjectNode innerNode = mapper.createObjectNode();
						innerNode.put(Schema.Type.LONG.getName(), longVal);
						updatedNode.set(fieldName, innerNode);	
					} else {
						updatedNode.put(fieldName, longVal);
					}
				} else {
					updatedNode.set(fieldName, null);
				}
				break;
			case DOUBLE:
				JsonNode doubleValue = valueNode.get(field.name());
				if (doubleValue != null) {
					if(isUnionSubtype) {
						ObjectNode innerNode = mapper.createObjectNode();
						innerNode.put(Schema.Type.DOUBLE.getName(), doubleValue.asDouble());
						updatedNode.set(fieldName, innerNode);	
					} else {
						updatedNode.put(fieldName, doubleValue.asDouble());
					}
				} else {
					updatedNode.set(fieldName, null);
				}
				break;
			case BOOLEAN:
				JsonNode boolValue = valueNode.get(fieldName);
				if (boolValue != null) {
					if(isUnionSubtype) {
						ObjectNode innerNode = mapper.createObjectNode();
						innerNode.put(Schema.Type.BOOLEAN.getName(), boolValue.asBoolean());
						updatedNode.set(fieldName, innerNode);	
					} else {
						updatedNode.put(fieldName, boolValue.asBoolean());
					}
				} else {
					updatedNode.set(fieldName, null);
				}
				break;
			default:
				log.error(valueSchema.getName() + "not defined" + valueSchema.getType());
			}
		}

		/**
		 * Update value as per array schema.
		 *
		 * @param field
		 *            the field
		 * @param valueSchema
		 *            the value schema
		 * @param valueNode
		 *            the value node
		 * @param updatedNode
		 *            the updated node
		 * @param isUnionSubtype
		 *            the is union subtype
		 * @param fieldName
		 *            the field name
		 */
		private void updateValueAsPerArraySchema(Schema.Field field, Schema valueSchema, ObjectNode valueNode,
				ObjectNode updatedNode, boolean isUnionSubtype, String fieldName) {
			ArrayNode arrayValue = (ArrayNode) valueNode.get(fieldName);
			ArrayNode updatedArrayNode = mapper.createArrayNode();
			Schema arrayElementType = valueSchema.getElementType();
			Schema unionSch = null;
			for (Schema unionSchema : arrayElementType.getTypes()) {
				if (unionSchema.getType().equals(Type.NULL)) {
					continue;
				} else {
					unionSch = unionSchema;
				}
			}
			if (arrayValue != null) {
				for (JsonNode arrayElement : arrayValue) {
					if (!arrayElement.isNull()) {
						ObjectNode innerNode = mapper.createObjectNode();
						if (unionSch.getType().getName().equals(Schema.Type.RECORD.name().toLowerCase())) {
							ObjectNode recordValueNode = mapper.createObjectNode();
							recordValueNode.set(field.name(), arrayElement);
							updateValueAsPerAvroSchema(field, unionSch, (ObjectNode) recordValueNode, innerNode,
									Boolean.FALSE);
						} else {
							innerNode.set(unionSch.getType().getName(), arrayElement);
						}
						updatedArrayNode.add(innerNode);
					} else {
						updatedArrayNode.add(arrayElement);
					}
				}
				if (isUnionSubtype) {
					ObjectNode outerNode = mapper.createObjectNode();
					outerNode.set(Schema.Type.ARRAY.getName(), updatedArrayNode);
					updatedNode.set(fieldName, outerNode);
				} else {
					updatedNode.set(fieldName, updatedArrayNode);
				}
			} else {
				updatedNode.set(fieldName, null);
			}
		}
	}
}
