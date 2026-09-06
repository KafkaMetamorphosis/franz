import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { setSessionToken } from "../api/client";

// The console auth is a stub (02.10 allow-all). "Signing in" just records an
// account label and installs a placeholder bearer token; the backend ignores it.

type Session = { account: string; email: string };

type AuthValue = {
  session: Session | null;
  signIn: (session: Session) => void;
  signOut: () => void;
};

const AuthContext = createContext<AuthValue | null>(null);

const STORAGE_KEY = "franz.console.session";

function loadSession(): Session | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as Session) : null;
  } catch {
    return null;
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(() => {
    const s = loadSession();
    if (s) setSessionToken(`console-stub:${s.account}`);
    return s;
  });

  const signIn = useCallback((next: Session) => {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    setSessionToken(`console-stub:${next.account}`);
    setSession(next);
  }, []);

  const signOut = useCallback(() => {
    sessionStorage.removeItem(STORAGE_KEY);
    setSessionToken(null);
    setSession(null);
  }, []);

  const value = useMemo<AuthValue>(() => ({ session, signIn, signOut }), [session, signIn, signOut]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components -- hook co-located with its provider
export function useAuth(): AuthValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
