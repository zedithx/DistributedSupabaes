package cluster

import (
	"html/template"
	"net/http"
)

var dashboardTemplate = template.Must(template.New("dashboard").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SupaSwarm Dashboard</title>
  <style>
    :root {
      --bg: #07131f;
      --panel: rgba(12, 28, 42, 0.84);
      --panel-2: rgba(17, 37, 56, 0.92);
      --line: rgba(148, 182, 201, 0.18);
      --text: #f3fbff;
      --muted: #9bb5c8;
      --accent: #5ae3c5;
      --accent-2: #7fc8ff;
      --danger: #ff7f9f;
      --warn: #ffc96c;
      --shadow: 0 30px 70px rgba(0, 0, 0, 0.35);
      font-family: "SF Mono", "Menlo", "Monaco", monospace;
    }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(90, 227, 197, 0.12), transparent 30%),
        radial-gradient(circle at top right, rgba(127, 200, 255, 0.14), transparent 28%),
        linear-gradient(180deg, #05101a 0%, #081521 38%, #07111a 100%);
    }
    main {
      max-width: 1240px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      display: grid;
      gap: 16px;
      margin-bottom: 24px;
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(2rem, 3vw, 3.1rem);
      letter-spacing: -0.05em;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 760px;
      line-height: 1.6;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
      padding: 18px;
      backdrop-filter: blur(14px);
    }
    .card h2 {
      margin-top: 0;
      font-size: 1rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .hero-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }
    .stat {
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
    }
    .label {
      color: var(--muted);
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .value {
      margin-top: 8px;
      font-size: 1.3rem;
      font-weight: 700;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }
    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.1em;
      font-size: 0.75rem;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 6px 12px;
      background: rgba(90, 227, 197, 0.12);
      border: 1px solid rgba(90, 227, 197, 0.22);
    }
    .danger {
      background: rgba(255, 127, 159, 0.12);
      border-color: rgba(255, 127, 159, 0.22);
    }
    .warn {
      background: rgba(255, 201, 108, 0.12);
      border-color: rgba(255, 201, 108, 0.22);
    }
    pre {
      margin: 0;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      line-height: 1.5;
    }
    .muted {
      color: var(--muted);
    }
    .member-links {
      display: grid;
      gap: 10px;
    }
    .member-link {
      display: block;
      text-decoration: none;
      color: var(--text);
      background: var(--panel-2);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
    }
    .member-link small {
      display: block;
      margin-top: 6px;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>SupaSwarm Control Plane</h1>
      <p>A single control-plane view of cluster membership, leader election, Lamport time, replicated log progress, and distributed lease ownership. Any node can serve this page, but it will prefer the leader's membership view so the dashboard stays canonical.</p>
      <div class="hero-meta">
        <span class="pill" id="controlPlaneSource">Discovering control plane...</span>
        <span class="pill warn" id="controlPlaneMode">Waiting for cluster membership...</span>
      </div>
    </section>

    <section class="grid">
      <article class="card">
        <h2>Cluster Snapshot</h2>
        <div class="stats">
          <div class="stat">
            <div class="label">Source Node</div>
            <div class="value" id="nodeId">-</div>
          </div>
          <div class="stat">
            <div class="label">Leader</div>
            <div class="value" id="leaderId">-</div>
          </div>
          <div class="stat">
            <div class="label">Term</div>
            <div class="value" id="term">-</div>
          </div>
          <div class="stat">
            <div class="label">Lamport</div>
            <div class="value" id="lamport">-</div>
          </div>
          <div class="stat">
            <div class="label">Commit Index</div>
            <div class="value" id="commitIndex">-</div>
          </div>
          <div class="stat">
            <div class="label">Last Log Index</div>
            <div class="value" id="lastLogIndex">-</div>
          </div>
        </div>
      </article>

      <article class="card">
        <h2>Lease</h2>
        <div id="leasePanel" class="muted">No active lease.</div>
      </article>
    </section>

    <section class="grid" style="margin-top: 16px;">
      <article class="card">
        <h2>Discovered Nodes</h2>
        <div id="memberLinks" class="member-links">
          <span class="muted">No nodes discovered yet.</span>
        </div>
      </article>

      <article class="card">
        <h2>Membership</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Address</th>
              <th>Status</th>
              <th>Last Applied</th>
              <th>Last Committed</th>
            </tr>
          </thead>
          <tbody id="membersBody"></tbody>
        </table>
      </article>

      <article class="card">
        <h2>Key-Value State</h2>
        <pre id="dataState">{}</pre>
      </article>
    </section>

    <section class="grid" style="margin-top: 16px;">
      <article class="card">
        <h2>Replicated Log</h2>
        <pre id="logState">[]</pre>
      </article>
    </section>
  </main>

  <script>
    function safe(value) {
      return value === null || value === undefined || value === "" ? "-" : value;
    }

    function pillForStatus(status) {
      const lower = (status || "").toLowerCase();
      if (lower === "leader") return "pill";
      if (lower === "suspect") return "pill danger";
      return "pill warn";
    }

    function htmlEscape(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    async function refresh() {
      const response = await fetch("/api/v1/control-plane");
      const data = await response.json();

      document.getElementById("nodeId").textContent = safe(data.sourceNodeId);
      document.getElementById("leaderId").textContent = safe(data.leaderId);
      document.getElementById("term").textContent = safe(data.currentTerm);
      document.getElementById("lamport").textContent = safe(data.lamport);
      document.getElementById("commitIndex").textContent = safe(data.commitIndex);
      document.getElementById("lastLogIndex").textContent = safe(data.lastLogIndex);
      document.getElementById("controlPlaneSource").textContent =
        "Control plane source: node " + safe(data.sourceNodeId) + " (" + safe(data.sourceAdvertiseAddr) + ")";
      document.getElementById("controlPlaneMode").textContent = data.degraded
        ? "Degraded: leader unreachable, showing local cluster view"
        : (data.servedByLeader
          ? "Canonical leader view discovered from membership"
          : "Local view");

      const lease = data.lease;
      document.getElementById("leasePanel").innerHTML = lease
        ? '<div class="pill">owner=' + safe(lease.owner) + ' name=' + safe(lease.name) + ' expires=' + safe(lease.expiresAt) + '</div><div class="muted" style="margin-top:10px;">Lease discovery flows through the replicated membership/log state.</div>'
        : '<span class="muted">No active lease.</span>';

      const tbody = document.getElementById("membersBody");
      tbody.innerHTML = "";
      (data.members || []).forEach(member => {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td>" + safe(member.id) + "</td>" +
          "<td>" + safe(member.address) + "</td>" +
          '<td><span class="' + pillForStatus(member.status) + '">' + safe(member.status) + "</span></td>" +
          "<td>" + safe(member.lastApplied) + "</td>" +
          "<td>" + safe(member.lastCommitted) + "</td>";
        tbody.appendChild(tr);
      });

      const memberLinks = document.getElementById("memberLinks");
      memberLinks.innerHTML = "";
      (data.members || []).forEach(member => {
        const card = document.createElement("div");
        card.className = "member-link";
        card.innerHTML =
          '<strong>Node ' + safe(member.id) + '</strong>' +
          ' <span class="' + pillForStatus(member.status) + '">' + safe(member.status) + "</span>" +
          '<small>Discovered from cluster membership: ' + htmlEscape(safe(member.address)) + "</small>" +
          '<small>Last committed index: ' + htmlEscape(safe(member.lastCommitted)) + '</small>';
        memberLinks.appendChild(card);
      });
      if ((data.members || []).length === 0) {
        memberLinks.innerHTML = '<span class="muted">No nodes discovered yet.</span>';
      }

      document.getElementById("dataState").textContent = JSON.stringify(data.data || {}, null, 2);
      document.getElementById("logState").textContent = JSON.stringify(data.log || [], null, 2);
    }

    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 1000);
  </script>
</body>
</html>`))

func (n *Node) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/dashboard" {
		http.NotFound(w, r)
		return
	}
	_ = dashboardTemplate.Execute(w, nil)
}
