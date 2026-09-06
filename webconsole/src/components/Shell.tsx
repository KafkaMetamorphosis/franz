import type { ReactNode } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

// The console shell — ported from 001-ux/demo/home.html. Nav groups that belong
// to features not built yet (Async Channels, Governance) are shown disabled so
// the information architecture stays recognisable.
export function Shell({ children }: { children: ReactNode }) {
  const { session, signOut } = useAuth();
  const navigate = useNavigate();

  return (
    <>
      <header className="topbar">
        <NavLink className="brand" to="/">
          <span className="brand-mark">F</span> Franz
        </NavLink>
        <div className="product">Console</div>
        <div className="global-search" />
        <nav className="topbar-actions" aria-label="Account navigation">
          <span>{session?.account}</span>
          <a
            href="#sign-out"
            onClick={(e) => {
              e.preventDefault();
              signOut();
              navigate("/login");
            }}
          >
            Sign out
          </a>
        </nav>
      </header>
      <div className="shell">
        <nav className="sidebar" aria-label="Main navigation">
          <NavLink className="nav-link" to="/" end>
            Home
          </NavLink>
          <details className="nav-group">
            <summary>Async Channels</summary>
            <span className="nav-link" aria-disabled="true" style={{ color: "#9aa4ad" }}>
              Coming with the feature
            </span>
          </details>
          <details className="nav-group">
            <summary>Governance</summary>
            <span className="nav-link" aria-disabled="true" style={{ color: "#9aa4ad" }}>
              Coming with the feature
            </span>
          </details>
          <details className="nav-group" open>
            <summary>Kafka</summary>
            <NavLink className="nav-link" to="/kafka/clusters">
              Clusters
            </NavLink>
            <NavLink className="nav-link" to="/agents">
              Agents
            </NavLink>
          </details>
        </nav>
        <main className="content">{children}</main>
      </div>
    </>
  );
}
