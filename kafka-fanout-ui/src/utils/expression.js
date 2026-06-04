/**
 * Human-readable preview line for a match condition.
 *
 * Examples:
 *   Message.TableName equals "Cattles" (case-insensitive)
 *   country contains "us" [CI]
 *   items[0].id not_equals "x" OR "y"
 */
export function previewMatchCondition({ key_path, operator, values = [], case_insensitive }) {
  if (!values || values.length === 0) {
    return `${key_path || ''} ${operator || ''} (no values)`;
  }
  const opLabel = operator === 'not_equals' ? 'not_equals' : operator;
  const ci = case_insensitive ? ' [CI]' : '';
  const quoted = values.map((v) => `"${v.value != null ? v.value : ''}"`).join(' OR ');
  return `${key_path} ${opLabel} ${quoted}${ci}`;
}
