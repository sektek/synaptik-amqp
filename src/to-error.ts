import { isError } from '@sektek/utility-belt';

/**
 * Normalizes a thrown value to an `Error` for lifecycle error events.
 *
 * @param thrown - The value that was thrown.
 * @returns `thrown` if it is an `Error`, otherwise an `Error` wrapping it as `cause`.
 */
export function toError(thrown: unknown): Error {
  return isError(thrown)
    ? thrown
    : new Error(String(thrown), { cause: thrown });
}
