import { Navigate, Route, Routes } from "react-router-dom";
import { useAuth } from "./auth/AuthContext";
import { Shell } from "./components/Shell";
import { Login } from "./pages/Login";
import { Home } from "./pages/Home";
import { AgentList } from "./pages/agents/AgentList";
import { AgentRegister } from "./pages/agents/AgentRegister";
import { AgentDetail } from "./pages/agents/AgentDetail";
import { AgentEdit } from "./pages/agents/AgentEdit";
import { ClusterList } from "./pages/clusters/ClusterList";
import { ClusterRegister } from "./pages/clusters/ClusterRegister";
import { ClusterDetail } from "./pages/clusters/ClusterDetail";
import { ClusterEdit } from "./pages/clusters/ClusterEdit";

export function App() {
  const { session } = useAuth();

  if (!session) {
    return (
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  }

  return (
    <Shell>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/agents" element={<AgentList />} />
        <Route path="/agents/register" element={<AgentRegister />} />
        <Route path="/agents/:name" element={<AgentDetail />} />
        <Route path="/agents/:name/edit" element={<AgentEdit />} />
        <Route path="/kafka/clusters" element={<ClusterList />} />
        <Route path="/kafka/clusters/register" element={<ClusterRegister />} />
        <Route path="/kafka/clusters/:name" element={<ClusterDetail />} />
        <Route path="/kafka/clusters/:name/edit" element={<ClusterEdit />} />
        <Route path="/login" element={<Navigate to="/" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Shell>
  );
}
