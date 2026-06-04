import { newId } from './id.js';

/**
 * The standard set of domain-grouping names the user's spreadsheet
 * uses. The dropdown in the UI pre-populates from this list, but the
 * name field is freeform — users can type any other name in the
 * "Other…" option.
 */
export const DOMAIN_GROUPING_NAMES = [
  'Registration',
  'Breeding',
  'Health',
  'Association',
  'Common',
  'Multiple',
  'Connector',
  'Stockist',
];

/** Make a fresh domain-grouping row in client-edit shape. */
export function newDomainGrouping() {
  return {
    name: '',
    match_conditions: [newMatchCondition()],
    destinations: [newDestination()],
  };
}

/** Make a fresh match condition: one key_path, one operator, one value. */
export function newMatchCondition() {
  return {
    key_path: 'Message.TableName',
    operator: 'equals',
    case_insensitive: true,
    values: [newMatchConditionValue()],
  };
}

/** A single value in a match condition's OR-list. */
export function newMatchConditionValue() {
  return { value: '' };
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

/** Make a fresh empty env with one default DG. */
export function newEnv() {
  const dg = newDomainGrouping();
  dg.destinations[0].topic = 'example.destination';
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
    domain_groupings: [dg],
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
