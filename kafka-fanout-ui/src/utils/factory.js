import { newId } from './id.js';

/** Make a fresh mapping row in client-edit shape (no id needed; backend assigns). */
export function newMapping() {
  return {
    key_path: 'Message.TableName',
    operator: 'equals',
    value: '',
    case_insensitive: true,
    destinations: [],
  };
}

/** Make a fresh destination row. */
export function newDestination() {
  return {
    use_source_broker: true,
    brokers: '',
    topic: '',
    security_protocol: 'PLAINTEXT',
    sasl_mechanism: null,
    sasl_username: '',
    sasl_password: null, // null = "not set"; '' literal = "touched but empty"
    ssl_ca_location: '',
    headers: [],
  };
}

/** Make a fresh header row. */
export function newHeader() {
  return {
    name: '',
    value: '',
    mode: 'static',
  };
}

/** Make a fresh empty env. */
export function newEnv() {
  return {
    name: '',
    description: '',
    enabled: false,
    dlq_topic: '',
    dlq_brokers: '',
    source: {
      brokers: '',
      topic: '',
      consumer_group: '',
      offset_reset: 'earliest',
      security_protocol: 'PLAINTEXT',
      sasl_mechanism: null,
      sasl_username: '',
      sasl_password: null,
      ssl_ca_location: '',
    },
    mappings: [],
  };
}

/** A placeholder shown in the password field once a secret has been saved. */
export const SECRET_PLACEHOLDER = '••••••••';

/** True if a saved secret exists (the API redacts it to null on read but
 * the source/destination rows carry a "previously saved" hint via a
 * transient flag on the client). */
export function hasSavedSecret(value) {
  return value === SECRET_PLACEHOLDER;
}
