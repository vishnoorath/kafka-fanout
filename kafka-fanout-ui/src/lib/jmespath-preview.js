import jmespath from 'jmespath';

/**
 * Translate our `@` root prefix into JMESPath's implicit-root form.
 * Mirrors the backend `matcher._preprocess`.
 *
 *   @.Message.TableName  ->  Message.TableName
 *   @                    ->  @
 *   @.items[0].id        ->  items[0].id
 */
function preprocess(expression) {
  if (!expression) return expression;
  const stripped = expression.replace(/^\s+/, '');
  const leadingWs = expression.slice(0, expression.length - stripped.length);
  if (!stripped.startsWith('@')) return expression;
  let body = stripped.slice(1);
  if (body.startsWith('.')) {
    body = body.slice(1);
  }
  return leadingWs + (body === '' ? '@' : body);
}

/**
 * Safe wrapper around jmespath.search.
 *
 * Returns `{ ok: true, value }` on success or `{ ok: false, error }` on
 * a parse / lex error. We don't enforce strict typing here — that's
 * the matcher's job on the backend, and the test panel renders the
 * distinction (matched / not matched / error) itself.
 */
export function safeSearch(expression, data) {
  try {
    const value = jmespath.search(data, preprocess(expression));
    return { ok: true, value };
  } catch (exc) {
    return { ok: false, error: String(exc && exc.message ? exc.message : exc) };
  }
}
