/**
 * Human-readable preview line for a condition.
 *
 * Examples:
 *   Message.TableName equals "Cattles" (case-insensitive)
 *   country contains "us" [CI]
 *   items[0].id not_equals "x"
 */
export function previewCondition({ key_path, operator, value, case_insensitive }) {
  const opLabel = operator === 'not_equals' ? 'not_equals' : operator;
  let line = `${key_path} ${opLabel} "${value}"`;
  if (case_insensitive) line += ' [CI]';
  return line;
}
