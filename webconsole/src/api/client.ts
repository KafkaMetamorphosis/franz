import createClient, { type Middleware } from "openapi-fetch";
import type { paths } from "./schema";

// The REST gateway base. Dev + prod default to the page origin (Vite proxies
// /v1 in dev; a co-hosted gateway serves it in prod). VITE_API_BASE overrides
// for a separately-hosted gateway. Always absolute so URL parsing works in the
// jsdom test environment too.
const baseUrl =
  import.meta.env.VITE_API_BASE ||
  (typeof window !== "undefined" ? window.location.origin : "http://localhost");

// Session token holder. The console auth is an allow-all stub (02.10) — this is
// a placeholder credential the login screen sets and every request carries, so
// the wiring is ready when real auth (003.2) lands.
let sessionToken: string | null = null;
export function setSessionToken(token: string | null) {
  sessionToken = token;
}
export function getSessionToken() {
  return sessionToken;
}

const authMiddleware: Middleware = {
  onRequest({ request }) {
    if (sessionToken) {
      request.headers.set("authorization", `Bearer ${sessionToken}`);
    }
    return request;
  },
};

// Wrap fetch in a closure so tests that stub globalThis.fetch after module load
// still take effect (openapi-fetch otherwise captures the reference eagerly).
export const api = createClient<paths>({
  baseUrl,
  fetch: (...args) => globalThis.fetch(...args),
});
api.use(authMiddleware);

// ApiError normalises a gateway error body ({code, message, details}) into a
// throwable for TanStack Query.
export class ApiError extends Error {
  readonly status: number;
  readonly fieldViolations: { field: string; description: string }[];

  constructor(status: number, message: string, fieldViolations: { field: string; description: string }[] = []) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.fieldViolations = fieldViolations;
  }
}

type GatewayErrorBody = {
  message?: string;
  details?: { "@type"?: string; fieldViolations?: { field?: string; description?: string }[] }[];
};

export function toApiError(status: number, body: unknown): ApiError {
  const b = (body ?? {}) as GatewayErrorBody;
  const violations =
    b.details
      ?.flatMap((d) => d.fieldViolations ?? [])
      .map((v) => ({ field: v.field ?? "", description: v.description ?? "" })) ?? [];
  return new ApiError(status, b.message || `request failed (${status})`, violations);
}
