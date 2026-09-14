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
import { ChannelList } from "./pages/channels/ChannelList";
import { ChannelRegister } from "./pages/channels/ChannelRegister";
import { ChannelDetail } from "./pages/channels/ChannelDetail";
import { ChannelEdit } from "./pages/channels/ChannelEdit";
import { IndicatorList } from "./pages/governance/IndicatorList";
import { IndicatorRegister } from "./pages/governance/IndicatorRegister";
import { IndicatorDetail } from "./pages/governance/IndicatorDetail";
import { IndicatorEdit } from "./pages/governance/IndicatorEdit";
import { PolicyList } from "./pages/governance/PolicyList";
import { PolicyRegister } from "./pages/governance/PolicyRegister";
import { PolicyDetail } from "./pages/governance/PolicyDetail";
import { PolicyEdit } from "./pages/governance/PolicyEdit";
import { ClientList } from "./pages/clients/ClientList";
import { ClientRegister } from "./pages/clients/ClientRegister";
import { ClientDetail } from "./pages/clients/ClientDetail";
import { ClientEdit } from "./pages/clients/ClientEdit";

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
        <Route path="/async-channels" element={<ChannelList />} />
        <Route path="/async-channels/register" element={<ChannelRegister />} />
        <Route path="/async-channels/:name" element={<ChannelDetail />} />
        <Route path="/async-channels/:name/edit" element={<ChannelEdit />} />
        <Route path="/governance/indicators" element={<IndicatorList />} />
        <Route path="/governance/indicators/register" element={<IndicatorRegister />} />
        <Route path="/governance/indicators/:name" element={<IndicatorDetail />} />
        <Route path="/governance/indicators/:name/edit" element={<IndicatorEdit />} />
        <Route path="/governance/policies" element={<PolicyList />} />
        <Route path="/governance/policies/register" element={<PolicyRegister />} />
        <Route path="/governance/policies/:name" element={<PolicyDetail />} />
        <Route path="/governance/policies/:name/edit" element={<PolicyEdit />} />
        <Route path="/clients" element={<ClientList />} />
        <Route path="/clients/register" element={<ClientRegister />} />
        <Route path="/clients/:name" element={<ClientDetail />} />
        <Route path="/clients/:name/edit" element={<ClientEdit />} />
        <Route path="/login" element={<Navigate to="/" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Shell>
  );
}
