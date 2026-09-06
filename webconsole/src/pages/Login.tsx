import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

export function Login() {
  const { signIn } = useAuth();
  const navigate = useNavigate();
  const [account, setAccount] = useState("");
  const [email, setEmail] = useState("");

  return (
    <div className="login-page">
      <main className="login-card" aria-labelledby="sign-in-title">
        <div className="login-brand">
          <span className="brand-mark">F</span> Franz
        </div>
        <h1 id="sign-in-title">Sign in to the Franz Console</h1>
        <p className="lead">Manage your async fleet&rsquo;s declared intent.</p>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            signIn({ account: account.trim() || "default", email: email.trim() });
            navigate("/");
          }}
        >
          <div className="login-field">
            <label htmlFor="account">Organization or account ID</label>
            <input
              id="account"
              autoComplete="organization"
              placeholder="acme-platform"
              value={account}
              onChange={(e) => setAccount(e.target.value)}
              required
            />
          </div>
          <div className="login-field">
            <label htmlFor="email">Email address</label>
            <input
              id="email"
              type="email"
              autoComplete="email"
              placeholder="you@acme.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <button className="button primary" type="submit">
            Sign in
          </button>
        </form>
        <p className="login-meta">
          Authentication is a stub &mdash; any values sign you in. Real identity comes with 003.2.
        </p>
      </main>
    </div>
  );
}
