import { useState, useEffect, useRef, useCallback } from "react";

// ------------------------------------------------------------------ //
// API key management
// The dashboard stores the API_SECRET in localStorage so you only
// enter it once. All /api/* requests send X-Api-Key with every call.
// ------------------------------------------------------------------ //
const LS_KEY = "canvas_agent_api_key";

function getStoredKey() {
  try { return localStorage.getItem(LS_KEY) || ""; } catch { return ""; }
}
function storeKey(k) {
  try { localStorage.setItem(LS_KEY, k); } catch {}
}

// ------------------------------------------------------------------ //
// API helpers — every call attaches X-Api-Key
// ------------------------------------------------------------------ //
function makeApi(apiKey) {
  const headers = () => ({ "Content-Type": "application/json", "X-Api-Key": apiKey });
  const baseHeaders = () => ({ "X-Api-Key": apiKey });

  const checkAuth = async (res) => {
    if (res.status === 401) throw new AuthError("Invalid API key");
    return res;
  };

  return {
    get: (path) =>
      fetch(`/api${path}`, { headers: headers() })
        .then(checkAuth)
        .then((r) => r.json()),

    post: (path, body) =>
      fetch(`/api${path}`, {
        method: "POST",
        headers: headers(),
        body: JSON.stringify(body),
      })
        .then(checkAuth)
        .then((r) => r.json()),

    upload: (path, file) => {
      const fd = new FormData();
      fd.append("file", file);
      return fetch(`/api${path}`, { method: "POST", headers: baseHeaders(), body: fd })
        .then(checkAuth)
        .then((r) => r.json());
    },

    uploadWithFields: (path, file, fields = {}) => {
      const fd = new FormData();
      fd.append("file", file);
      const url = new URL(`/api${path}`, window.location.origin);
      Object.entries(fields).forEach(([k, v]) => v && url.searchParams.set(k, v));
      return fetch(url.toString(), { method: "POST", headers: baseHeaders(), body: fd })
        .then(checkAuth)
        .then((r) => r.json());
    },
  };
}

class AuthError extends Error {}

// ------------------------------------------------------------------ //
// Setup screen — shown when no API key is stored
// ------------------------------------------------------------------ //
function SetupScreen({ onSave }) {
  const [key, setKey] = useState("");
  const [error, setError] = useState("");
  const [testing, setTesting] = useState(false);

  const test = async () => {
    if (!key.trim()) { setError("Enter your API key first."); return; }
    setTesting(true);
    setError("");
    try {
      const res = await fetch("/api/health", {
        headers: { "X-Api-Key": key.trim(), "Content-Type": "application/json" },
      });
      if (res.status === 401) { setError("Key rejected — check it matches API_SECRET in Railway."); }
      else if (res.ok) { storeKey(key.trim()); onSave(key.trim()); }
      else { setError(`Unexpected response: ${res.status}`); }
    } catch (e) {
      setError("Could not reach the server. Is your Railway URL correct?");
    }
    setTesting(false);
  };

  return (
    <div style={{ minHeight: "100vh", background: "#0d1117", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ width: 420, background: "#111827", borderRadius: 16, padding: 36, border: "1px solid #1f2937" }}>
        <div style={{ fontSize: 18, fontWeight: 700, color: "#f9fafb", marginBottom: 8 }}>Canvas Agent</div>
        <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 28, lineHeight: 1.6 }}>
          Enter the API secret you set in your Railway Variables to access the dashboard.
        </div>

        <label style={{ fontSize: 12, color: "#6b7280", display: "block", marginBottom: 6 }}>API SECRET KEY</label>
        <input
          type="password"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && test()}
          placeholder="Paste your API_SECRET here"
          style={{
            width: "100%", padding: "10px 14px", background: "#0d1117",
            border: `1px solid ${error ? "#ef4444" : "#374151"}`, borderRadius: 8,
            color: "#f9fafb", fontSize: 13, fontFamily: "inherit",
            outline: "none", boxSizing: "border-box", marginBottom: 8,
          }}
        />
        {error && <div style={{ fontSize: 12, color: "#ef4444", marginBottom: 12 }}>{error}</div>}

        <button
          onClick={test}
          disabled={testing}
          style={{
            width: "100%", padding: "10px 0", background: testing ? "#374151" : "#1d4ed8",
            color: testing ? "#6b7280" : "#fff", border: "none", borderRadius: 8,
            fontSize: 14, fontWeight: 500, cursor: testing ? "default" : "pointer",
            fontFamily: "inherit", marginTop: 4,
          }}
        >
          {testing ? "Verifying…" : "Connect"}
        </button>

        <div style={{ marginTop: 20, padding: 14, background: "#0d1117", borderRadius: 8, fontSize: 12, color: "#4b5563", lineHeight: 1.6 }}>
          <strong style={{ color: "#6b7280" }}>Don't have a key?</strong><br />
          Generate one in your terminal:<br />
          <code style={{ color: "#9ca3af" }}>python -c "import secrets; print(secrets.token_hex(32))"</code><br /><br />
          Then add it as <code style={{ color: "#9ca3af" }}>API_SECRET</code> in Railway Variables.
        </div>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
// Nav
// ------------------------------------------------------------------ //
const NAV = [
  { id: "dashboard",   label: "Dashboard"    },
  { id: "chat",        label: "Chat"         },
  { id: "assignments", label: "Assignments"  },
  { id: "knowledge",   label: "Knowledge"    },
  { id: "documents",   label: "Documents"    },
  { id: "voice",       label: "Voice & Style"},
  { id: "system",      label: "System"       },
  { id: "snapshots",   label: "Snapshots"    },
];

// ------------------------------------------------------------------ //
// Dashboard panel
// ------------------------------------------------------------------ //
function DashboardPanel({ api }) {
  const [health, setHealth] = useState(null);
  const [briefing, setBriefing] = useState(null);
  const [cookieStatus, setCookieStatus] = useState(null);
  const [crawling, setCrawling] = useState(false);
  const [cookieFile, setCookieFile] = useState(null);

  const refresh = useCallback(async () => {
    try {
      const [h, cs] = await Promise.all([api.get("/health"), api.get("/cookies/status")]);
      setHealth(h);
      setCookieStatus(cs);
    } catch {}
  }, [api]);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 10000);
    return () => clearInterval(t);
  }, [refresh]);

  const uploadCookies = async () => {
    if (!cookieFile) return;
    const res = await api.upload("/cookies/upload", cookieFile);
    alert(res.message || res.detail);
    refresh();
  };

  const startCrawl = async () => {
    setCrawling(true);
    try {
      const res = await api.post("/crawl/start", {});
      if (res.detail) { alert(res.detail); setCrawling(false); return; }
      const poll = setInterval(async () => {
        const status = await api.get("/crawl/status");
        if (!status.running) {
          clearInterval(poll);
          setCrawling(false);
          refresh();
          alert(status.message);
        }
      }, 3000);
    } catch (e) {
      setCrawling(false);
      alert("Crawl failed: " + e.message);
    }
  };

  const loadBriefing = async () => {
    setBriefing("Loading...");
    const res = await api.get("/knowledge/briefing");
    setBriefing(res.briefing);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
        <StatCard label="Assignments"    value={health?.knowledge_base?.assignments ?? "—"} />
        <StatCard label="Documents"      value={health?.knowledge_base?.documents ?? "—"} />
        <StatCard label="Course Content" value={health?.knowledge_base?.course_content ?? "—"} />
        <StatCard label="Review Queue"   value={health?.queue_size ?? "—"} />
      </div>

      <Section title="Canvas Session">
        {cookieStatus?.valid ? (
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 10 }}>
            <span style={{ color: "#22c55e", fontSize: 13 }}>Session active — {cookieStatus.cookie_count} cookies</span>
            <span style={{ color: "#6b7280", fontSize: 12 }}>Exported {cookieStatus.exported_at?.slice(0, 10)}</span>
          </div>
        ) : (
          <div style={{ color: "#f59e0b", fontSize: 13, marginBottom: 10 }}>No session — upload canvas_cookies.json</div>
        )}
        <div style={{ display: "flex", gap: 8 }}>
          <input type="file" accept=".json" onChange={(e) => setCookieFile(e.target.files[0])}
            style={{ fontSize: 13, flex: 1, color: "#d1d5db" }} />
          <Btn onClick={uploadCookies} disabled={!cookieFile}>Upload</Btn>
        </div>
      </Section>

      <Section title="Knowledge Base">
        <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
          {health?.crawl_status?.message || "Ready to crawl"}
          {health?.crawl_status?.last_run && ` — Last run: ${health.crawl_status.last_run.slice(0, 16)}`}
        </p>
        <Btn onClick={startCrawl} disabled={crawling || !cookieStatus?.valid}>
          {crawling ? "Crawling Canvas…" : "Crawl Canvas Now"}
        </Btn>
      </Section>

      <Section title="Daily Briefing">
        <Btn onClick={loadBriefing} style={{ marginBottom: 12 }}>Generate Briefing</Btn>
        {briefing && (
          <div style={{ fontSize: 13, color: "#d1d5db", whiteSpace: "pre-wrap", lineHeight: 1.7, marginTop: 8 }}>
            {briefing}
          </div>
        )}
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ //
// Chat panel
// ------------------------------------------------------------------ //
function ChatPanel({ api }) {
  const [messages, setMessages] = useState([
    { role: "assistant", content: "Hello! I'm your Canvas AI Student Agent. I've been enrolled in your courses and I'm ready to help. What would you like to work on?" },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  const send = async () => {
    if (!input.trim() || loading) return;
    const msg = input.trim();
    setInput("");
    setMessages((m) => [...m, { role: "user", content: msg }]);
    setLoading(true);
    try {
      const res = await api.post("/chat", { message: msg });
      setMessages((m) => [...m, { role: "assistant", content: res.reply || res.detail || "Error" }]);
    } catch {
      setMessages((m) => [...m, { role: "assistant", content: "Error connecting to agent. Try again." }]);
    }
    setLoading(false);
  };

  const reset = async () => {
    await api.post("/chat/reset", { confirm: true });
    setMessages([{ role: "assistant", content: "Conversation reset. How can I help?" }]);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "calc(100vh - 120px)" }}>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 12 }}>
        <button onClick={reset} style={{ fontSize: 12, color: "#6b7280", background: "none", border: "none", cursor: "pointer" }}>
          Reset conversation
        </button>
      </div>
      <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 16, paddingRight: 4 }}>
        {messages.map((m, i) => (
          <div key={i} style={{ display: "flex", justifyContent: m.role === "user" ? "flex-end" : "flex-start" }}>
            <div style={{
              maxWidth: "72%", padding: "12px 16px",
              borderRadius: m.role === "user" ? "18px 18px 4px 18px" : "18px 18px 18px 4px",
              background: m.role === "user" ? "#1d4ed8" : "#1f2937",
              color: "#f9fafb", fontSize: 14, lineHeight: 1.65, whiteSpace: "pre-wrap",
            }}>
              {m.content}
            </div>
          </div>
        ))}
        {loading && (
          <div style={{ display: "flex" }}>
            <div style={{ padding: "12px 16px", borderRadius: "18px 18px 18px 4px", background: "#1f2937", color: "#6b7280", fontSize: 14 }}>
              Thinking…
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>
      <div style={{ display: "flex", gap: 8, marginTop: 16 }}>
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
          placeholder="Ask about assignments, course material, or start working on something…"
          rows={3}
          style={{
            flex: 1, padding: "12px 16px", borderRadius: 12, border: "1px solid #374151",
            background: "#111827", color: "#f9fafb", fontSize: 14, resize: "none",
            outline: "none", lineHeight: 1.5, fontFamily: "inherit",
          }}
        />
        <Btn onClick={send} disabled={loading} style={{ alignSelf: "flex-end", padding: "12px 20px" }}>Send</Btn>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
// Assignments panel
// ------------------------------------------------------------------ //
function AssignmentsPanel({ api }) {
  const [assignments, setAssignments] = useState([]);
  const [queue, setQueue] = useState([]);
  const [selected, setSelected] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [generating, setGenerating] = useState(false);
  const [tab, setTab] = useState("upcoming");

  const loadData = useCallback(() => {
    api.get("/knowledge/upcoming").then(setAssignments).catch(() => {});
    api.get("/assignments/queue").then(setQueue).catch(() => {});
  }, [api]);

  useEffect(() => { loadData(); }, [loadData]);

  const analyze = async (a) => {
    setSelected(a);
    setAnalysis(null);
    const id = a.metadata?.assignment_id;
    const res = await api.post(`/assignments/analyze/${encodeURIComponent(id)}`, {});
    setAnalysis(res.analysis);
  };

  const generate = async (a) => {
    setGenerating(true);
    const id = a.metadata?.assignment_id;
    try {
      const res = await api.post(`/assignments/generate/${encodeURIComponent(id)}`, {});
      if (res.mode === "copilot") {
        alert("Co-pilot mode: this assignment needs to be worked through in Chat.");
      } else {
        alert(`Draft ready! Check the Review Queue.`);
      }
      loadData();
    } catch (e) {
      alert("Generation failed: " + e.message);
    }
    setGenerating(false);
  };

  const approve = async (draftId, approved) => {
    const feedback = approved ? null : prompt("What needs to change?");
    await api.post("/assignments/approve", { assignment_id: draftId, approved, feedback });
    loadData();
  };

  return (
    <div>
      <div style={{ display: "flex", gap: 4, marginBottom: 20 }}>
        {["upcoming", "queue"].map((t) => (
          <button key={t} onClick={() => setTab(t)} style={{
            padding: "6px 16px", borderRadius: 20, border: "none", cursor: "pointer",
            background: tab === t ? "#1d4ed8" : "#1f2937", color: "#f9fafb",
            fontSize: 13, fontFamily: "inherit", fontWeight: tab === t ? 600 : 400,
          }}>
            {t === "upcoming" ? `Upcoming (${assignments.length})` : `Review Queue (${queue.length})`}
          </button>
        ))}
      </div>

      {tab === "upcoming" && (
        <div>
          {assignments.length === 0 && <EmptyState label="No assignments found. Run a crawl first." />}
          {Object.entries(
            assignments.reduce((acc, a) => {
              const c = a.metadata?.course_name || "Unknown";
              if (!acc[c]) acc[c] = [];
              acc[c].push(a);
              return acc;
            }, {})
          ).map(([courseName, items]) => (
            <div key={courseName} style={{ marginBottom: 20 }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: "#6b7280", textTransform: "uppercase",
                letterSpacing: "0.08em", marginBottom: 8, paddingBottom: 6,
                borderBottom: "1px solid #1f2937" }}>
                {courseName} ({items.length})
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {items.map((a, i) => (
                  <div key={i} style={{ padding: 14, background: "#1f2937", borderRadius: 10,
                    display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <div>
                      <div style={{ fontWeight: 500, fontSize: 13, color: "#f9fafb" }}>{a.metadata?.title}</div>
                      <div style={{ fontSize: 11, color: "#6b7280", marginTop: 3 }}>
                        Due: {a.metadata?.due || "No due date"}
                        {a.metadata?.points ? ` · ${a.metadata.points}` : ""}
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
                      <Btn small onClick={() => analyze(a)}>Analyze</Btn>
                      <Btn small onClick={() => generate(a)} disabled={generating}>Generate</Btn>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {tab === "queue" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {queue.length === 0 && <EmptyState label="No drafts awaiting review." />}
          {queue.map((d, i) => (
            <div key={i} style={{ padding: 16, background: "#1f2937", borderRadius: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div>
                  <div style={{ fontWeight: 600, fontSize: 14, color: "#f9fafb" }}>{d.title}</div>
                  <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{d.course} · {d.file_type?.toUpperCase()} · Due: {d.due}</div>
                  <div style={{ fontSize: 12, marginTop: 6, color: d.status === "approved" ? "#22c55e" : d.status === "rejected" ? "#ef4444" : "#f59e0b" }}>
                    {d.status}
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
                  <a href={`/api/assignments/download/${d.id}`}
                    onClick={(e) => { e.preventDefault(); downloadDraft(d.id, api); }}
                    style={{ textDecoration: "none" }}>
                    <Btn small>Download</Btn>
                  </a>
                  {d.status === "awaiting_review" && (
                    <>
                      <Btn small onClick={() => approve(d.id, true)} style={{ background: "#166534" }}>Approve</Btn>
                      <Btn small onClick={() => approve(d.id, false)} style={{ background: "#7f1d1d" }}>Revise</Btn>
                    </>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {selected && analysis && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 100 }}>
          <div style={{ background: "#111827", borderRadius: 16, padding: 28, maxWidth: 600, width: "90%", maxHeight: "80vh", overflowY: "auto" }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
              <h3 style={{ margin: 0, fontSize: 16, color: "#f9fafb" }}>{selected.metadata?.title}</h3>
              <button onClick={() => { setSelected(null); setAnalysis(null); }}
                style={{ background: "none", border: "none", color: "#6b7280", cursor: "pointer", fontSize: 18 }}>x</button>
            </div>
            <pre style={{ fontSize: 12, color: "#d1d5db", whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
              {JSON.stringify(analysis, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

async function downloadDraft(draftId, api) {
  try {
    const resp = await fetch(`/api/assignments/download/${draftId}`, {
      headers: { "X-Api-Key": getStoredKey() },
    });
    if (!resp.ok) { alert("Download failed"); return; }
    const blob = await resp.blob();
    const cd = resp.headers.get("content-disposition") || "";
    const match = cd.match(/filename="?([^"]+)"?/);
    const filename = match ? match[1] : `draft_${draftId}`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  } catch (e) { alert("Download failed: " + e.message); }
}

// ------------------------------------------------------------------ //
// Knowledge panel
// ------------------------------------------------------------------ //
function KnowledgePanel({ api }) {
  const [tab, setTab] = useState("search");
  const [courses, setCourses] = useState([]);
  const [selectedCourse, setSelectedCourse] = useState("");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState(null);
  const [searching, setSearching] = useState(false);
  const [stats, setStats] = useState(null);
  const [summary, setSummary] = useState("");
  const [summarizing, setSummarizing] = useState(false);

  useEffect(() => {
    api.get("/knowledge/stats").then(setStats).catch(() => {});
    api.get("/knowledge/courses").then((r) => setCourses(r.courses || [])).catch(() => {});
  }, [api]);

  const search = async () => {
    if (!query.trim()) return;
    setSearching(true);
    setResults(null);
    try {
      const body = { message: query, course_name: selectedCourse || undefined };
      const [searchRes, docRes] = await Promise.all([
        api.post("/knowledge/search", body),
        api.post("/documents/search", body),
      ]);
      setResults({
        assignments: searchRes.assignments || [],
        content: searchRes.content || [],
        documents: docRes.results || [],
      });
    } catch {
      setResults({ assignments: [], content: [], documents: [] });
    }
    setSearching(false);
  };

  const generateSummary = async () => {
    setSummarizing(true);
    setSummary("");
    const courseClause = selectedCourse ? `for ${selectedCourse}` : "for all my courses";
    try {
      const res = await api.post("/chat", {
        message: `Give me a comprehensive knowledge summary ${courseClause}. Include: key themes and concepts, what the syllabus covers, major assignments, important readings and what they cover, and anything else you know from the course materials. Be specific and detailed.`,
      });
      setSummary(res.reply || "");
    } catch {
      setSummary("Error generating summary — make sure the AI model is configured.");
    }
    setSummarizing(false);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

      {/* Stats */}
      {stats && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10 }}>
          {[["Assignments", stats.assignments], ["Documents", stats.documents],
            ["Course Content", stats.course_content], ["Voice Samples", stats.voice_samples]].map(([label, val]) => (
            <div key={label} style={{ padding: "12px 16px", background: "#1f2937", borderRadius: 10 }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: "#f9fafb" }}>{val ?? "—"}</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>{label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Course selector */}
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div style={{ fontSize: 12, color: "#6b7280", flexShrink: 0 }}>Course:</div>
        <select value={selectedCourse} onChange={(e) => { setSelectedCourse(e.target.value); setResults(null); setSummary(""); }}
          style={{ flex: 1, padding: "8px 12px", background: "#1f2937", border: "1px solid #374151",
            borderRadius: 8, color: "#f9fafb", fontSize: 13, fontFamily: "inherit" }}>
          <option value="">All Courses</option>
          {courses.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 4 }}>
        {["search", "summary"].map((t) => (
          <button key={t} onClick={() => setTab(t)} style={{
            padding: "6px 16px", borderRadius: 20, border: "none", cursor: "pointer",
            background: tab === t ? "#1d4ed8" : "#1f2937", color: "#f9fafb",
            fontSize: 13, fontFamily: "inherit", fontWeight: tab === t ? 600 : 400,
          }}>
            {t === "search" ? "Search Knowledge" : "AI Summary"}
          </button>
        ))}
      </div>

      {tab === "search" && (
        <div>
          <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
            <input value={query} onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && search()}
              placeholder={`Search${selectedCourse ? ` in ${selectedCourse}` : " all courses"}…`}
              style={{ ...inputStyle, flex: 1 }} />
            <Btn onClick={search} disabled={searching || !query.trim()}>
              {searching ? "Searching…" : "Search"}
            </Btn>
          </div>
          {!results && (
            <div style={{ fontSize: 13, color: "#4b5563", textAlign: "center", padding: 32 }}>
              Search across syllabi, readings, documents, and assignments
              {selectedCourse ? ` for ${selectedCourse}` : ""}.
            </div>
          )}
          {results && (
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              {[
                { key: "documents",   label: "Documents & Readings" },
                { key: "content",     label: "Course Content" },
                { key: "assignments", label: "Assignments" },
              ].map(({ key, label }) =>
                results[key]?.length > 0 ? (
                  <div key={key}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: "#6b7280",
                      textTransform: "uppercase", letterSpacing: ".07em", marginBottom: 8 }}>
                      {label} ({results[key].length})
                    </div>
                    {results[key].map((r, i) => <KnowledgeCard key={i} result={r} />)}
                  </div>
                ) : null
              )}
              {!results.documents?.length && !results.content?.length && !results.assignments?.length && (
                <EmptyState label="No results found. Try different keywords." />
              )}
            </div>
          )}
        </div>
      )}

      {tab === "summary" && (
        <div>
          <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 14 }}>
            Generate an AI summary of everything indexed
            {selectedCourse ? ` for ${selectedCourse}` : " across all courses"}.
          </p>
          <Btn onClick={generateSummary} disabled={summarizing} style={{ marginBottom: 16 }}>
            {summarizing ? "Generating…" : `Summarize ${selectedCourse || "All Courses"}`}
          </Btn>
          {summary && (
            <div style={{ padding: 20, background: "#1f2937", borderRadius: 12,
              fontSize: 13, color: "#d1d5db", lineHeight: 1.8, whiteSpace: "pre-wrap" }}>
              {summary}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function KnowledgeCard({ result }) {
  const [expanded, setExpanded] = useState(false);
  const meta = result.metadata || {};
  const text = result.document || "";
  const preview = text.slice(0, 300);
  const hasMore = text.length > 300;
  return (
    <div style={{ padding: "12px 16px", background: "#1f2937", borderRadius: 10, marginBottom: 8 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 6 }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: "#f9fafb" }}>
            {meta.title || meta.assignment || meta.module_name || meta.type || "Content"}
          </div>
          <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>
            {meta.course_name || ""}
            {meta.doc_type ? ` · ${meta.doc_type}` : ""}
            {meta.type && meta.type !== meta.doc_type ? ` · ${meta.type}` : ""}
          </div>
        </div>
        {result.distance != null && (
          <div style={{ fontSize: 10, color: "#6b7280", background: "#111827",
            padding: "2px 8px", borderRadius: 20, flexShrink: 0 }}>
            {Math.round((1 - result.distance) * 100)}% match
          </div>
        )}
      </div>
      <div style={{ fontSize: 12, color: "#9ca3af", lineHeight: 1.6 }}>
        {expanded ? text : preview}{hasMore && !expanded && "…"}
      </div>
      {hasMore && (
        <button onClick={() => setExpanded(!expanded)}
          style={{ fontSize: 11, color: "#3b82f6", background: "none", border: "none", cursor: "pointer", padding: "4px 0 0" }}>
          {expanded ? "Show less" : "Show more"}
        </button>
      )}
    </div>
  );
}



// ------------------------------------------------------------------ //
// Documents panel — indexed docs, flagged links, manual upload
// ------------------------------------------------------------------ //
function DocumentsPanel({ api }) {
  const [courses, setCourses] = useState([]);
  const [selectedCourse, setSelectedCourse] = useState("");
  const [docTab, setDocTab] = useState("indexed");
  const [indexedDocs, setIndexedDocs] = useState([]);
  const [loadingDocs, setLoadingDocs] = useState(false);
  const [flagged, setFlagged] = useState([]);
  const [pasteTitle, setPasteTitle] = useState("");
  const [pasteText, setPasteText] = useState("");
  const [pasteCourse, setPasteCourse] = useState("");
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadTitle, setUploadTitle] = useState("");
  const [uploadCourse, setUploadCourse] = useState("");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    api.get("/knowledge/courses").then((r) => setCourses(r.courses || [])).catch(() => {});
    api.get("/documents/flagged").then((r) => setFlagged(r.flagged || [])).catch(() => {});
    loadIndexed("");
  }, [api]);

  const loadIndexed = async (course) => {
    setLoadingDocs(true);
    try {
      const url = course ? `/documents/list?course_name=${encodeURIComponent(course)}` : "/documents/list";
      const res = await api.get(url);
      setIndexedDocs(res.documents || []);
    } catch { setIndexedDocs([]); }
    setLoadingDocs(false);
  };

  const onCourseChange = (course) => {
    setSelectedCourse(course);
    if (docTab === "indexed") loadIndexed(course);
    if (docTab === "flagged") {
      const filt = course
        ? api.get(`/documents/flagged/${encodeURIComponent(course)}`).then((r) => setFlagged(r.flagged || [])).catch(() => {})
        : api.get("/documents/flagged").then((r) => setFlagged(r.flagged || [])).catch(() => {});
    }
  };

  const savePaste = async () => {
    if (!pasteTitle.trim() || !pasteText.trim() || !pasteCourse.trim()) {
      setMsg("Title, course, and text are all required."); return;
    }
    setSaving(true); setMsg("");
    try {
      const res = await api.post("/documents/upload", { title: pasteTitle, text: pasteText, course_name: pasteCourse });
      setMsg(res.message || "Added.");
      setPasteTitle(""); setPasteText(""); setPasteCourse("");
      loadIndexed(selectedCourse);
    } catch (e) { setMsg("Failed: " + e.message); }
    setSaving(false);
  };

  const saveFile = async () => {
    if (!uploadFile || !uploadCourse.trim()) { setMsg("Select a file and enter the course name."); return; }
    setSaving(true); setMsg("");
    try {
      const res = await api.uploadWithFields("/documents/upload-file", uploadFile, {
        course_name: uploadCourse, title: uploadTitle || undefined,
      });
      setMsg(res.message || "Uploaded.");
      setUploadFile(null); setUploadTitle(""); setUploadCourse("");
      loadIndexed(selectedCourse);
    } catch (e) { setMsg("Failed: " + e.message); }
    setSaving(false);
  };

  // Group indexed docs by doc_type for display
  const docGroups = indexedDocs.reduce((acc, d) => {
    const type = d.metadata?.doc_type || d.metadata?.source || "other";
    if (!acc[type]) acc[type] = [];
    acc[type].push(d);
    return acc;
  }, {});

  const typeLabel = (t) => ({
    canvas_file: "Canvas Files", html_page: "Canvas Pages", google_doc: "Google Docs",
    microsoft_doc: "OneDrive / SharePoint", google_drive: "Google Drive",
    manual_upload: "Manually Uploaded", web_page: "Web Pages",
  }[t] || t);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

      {/* Course selector */}
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div style={{ fontSize: 12, color: "#6b7280", flexShrink: 0 }}>Course:</div>
        <select value={selectedCourse} onChange={(e) => onCourseChange(e.target.value)}
          style={{ flex: 1, padding: "8px 12px", background: "#1f2937", border: "1px solid #374151",
            borderRadius: 8, color: "#f9fafb", fontSize: 13, fontFamily: "inherit" }}>
          <option value="">All Courses</option>
          {courses.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
        {["indexed", "flagged", "paste", "file"].map((t) => (
          <button key={t} onClick={() => { setDocTab(t); setMsg(""); if (t === "indexed") loadIndexed(selectedCourse); }} style={{
            padding: "6px 16px", borderRadius: 20, border: "none", cursor: "pointer",
            background: docTab === t ? "#1d4ed8" : "#1f2937", color: "#f9fafb",
            fontSize: 13, fontFamily: "inherit", fontWeight: docTab === t ? 600 : 400,
          }}>
            {t === "indexed" ? `Course Documents (${indexedDocs.length})` :
             t === "flagged" ? `Flagged (${flagged.length})` :
             t === "paste" ? "Paste Text" : "Upload File"}
          </button>
        ))}
      </div>

      {docTab === "indexed" && (
        <div>
          <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
            All documents the agent has read and indexed from Canvas
            {selectedCourse ? ` for ${selectedCourse}` : ""}.
          </p>
          {loadingDocs && <div style={{ fontSize: 13, color: "#6b7280" }}>Loading…</div>}
          {!loadingDocs && indexedDocs.length === 0 && (
            <EmptyState label="No documents indexed yet. Run a crawl first." />
          )}
          {Object.entries(docGroups).map(([type, docs]) => (
            <div key={type} style={{ marginBottom: 20 }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: "#6b7280",
                textTransform: "uppercase", letterSpacing: ".07em", marginBottom: 8 }}>
                {typeLabel(type)} ({docs.length})
              </div>
              {docs.map((d, i) => (
                <div key={i} style={{ padding: "10px 14px", background: "#1f2937", borderRadius: 8, marginBottom: 6 }}>
                  <div style={{ fontSize: 13, fontWeight: 500, color: "#f9fafb" }}>
                    {d.metadata?.title || "Untitled"}
                  </div>
                  <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>
                    {d.metadata?.course_name}
                    {d.metadata?.total_chunks > 1 ? ` · ${d.metadata.total_chunks} chunks` : ""}
                    {d.metadata?.char_count ? ` · ${Math.round(d.metadata.char_count / 1000)}K chars` : ""}
                  </div>
                  {d.metadata?.url && (
                    <a href={d.metadata.url} target="_blank" rel="noopener noreferrer"
                      style={{ fontSize: 11, color: "#3b82f6", marginTop: 3, display: "block" }}>
                      {d.metadata.url.slice(0, 80)}{d.metadata.url.length > 80 ? "…" : ""}
                    </a>
                  )}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}

      {docTab === "flagged" && (
        <div>
          <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
            Links found on Canvas that couldn't be accessed automatically. Add them manually below.
          </p>
          {flagged.length === 0 && <EmptyState label="No flagged external links found." />}
          {flagged.map((f, i) => (
            <div key={i} style={{ padding: 14, background: "#1f2937", borderRadius: 10, marginBottom: 8 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: "#f9fafb" }}>{f.metadata?.title || f.document}</div>
              <div style={{ fontSize: 12, color: "#f59e0b", marginTop: 4 }}>{f.metadata?.platform}</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>{f.metadata?.course_name}</div>
              {f.metadata?.url && (
                <a href={f.metadata.url} target="_blank" rel="noopener noreferrer"
                  style={{ fontSize: 11, color: "#3b82f6", marginTop: 4, display: "block", wordBreak: "break-all" }}>
                  {f.metadata.url}
                </a>
              )}
            </div>
          ))}
        </div>
      )}

      {docTab === "paste" && (
        <Section title="Paste Reading Text">
          <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 14 }}>
            Copy text from VitalSource, Pearson, or any external platform and paste it here.
          </p>
          <input value={pasteTitle} onChange={(e) => setPasteTitle(e.target.value)}
            placeholder="Document title" style={inputStyle} />
          <select value={pasteCourse} onChange={(e) => setPasteCourse(e.target.value)}
            style={{ ...inputStyle, marginTop: 8 }}>
            <option value="">Select course…</option>
            {courses.map((c) => <option key={c} value={c}>{c}</option>)}
            <option value="__custom">Other (type below)</option>
          </select>
          {pasteCourse === "__custom" && (
            <input onChange={(e) => setPasteCourse(e.target.value)}
              placeholder="Course name" style={{ ...inputStyle, marginTop: 8 }} />
          )}
          <textarea value={pasteText} onChange={(e) => setPasteText(e.target.value)}
            placeholder="Paste the reading content here…" rows={10}
            style={{ ...inputStyle, marginTop: 8, resize: "vertical" }} />
          {msg && <div style={{ fontSize: 12, color: msg.startsWith("Failed") ? "#ef4444" : "#22c55e", marginTop: 8 }}>{msg}</div>}
          <Btn onClick={savePaste} disabled={saving} style={{ marginTop: 10 }}>
            {saving ? "Saving…" : "Add to Knowledge Base"}
          </Btn>
        </Section>
      )}

      {docTab === "file" && (
        <Section title="Upload PDF or Word File">
          <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 14 }}>Upload a PDF or .docx file. Max 20 MB.</p>
          <input type="file" accept=".pdf,.docx,.txt" onChange={(e) => setUploadFile(e.target.files[0])}
            style={{ fontSize: 13, color: "#d1d5db", marginBottom: 8, display: "block" }} />
          <input value={uploadTitle} onChange={(e) => setUploadTitle(e.target.value)}
            placeholder="Document title (optional)" style={inputStyle} />
          <select value={uploadCourse} onChange={(e) => setUploadCourse(e.target.value)}
            style={{ ...inputStyle, marginTop: 8 }}>
            <option value="">Select course…</option>
            {courses.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          {msg && <div style={{ fontSize: 12, color: msg.startsWith("Failed") ? "#ef4444" : "#22c55e", marginTop: 8 }}>{msg}</div>}
          <Btn onClick={saveFile} disabled={saving || !uploadFile} style={{ marginTop: 10 }}>
            {saving ? "Uploading…" : "Upload and Index"}
          </Btn>
        </Section>
      )}
    </div>
  );
}


const inputStyle = {
  width: "100%", padding: "10px 14px", background: "#111827",
  border: "1px solid #374151", borderRadius: 8, color: "#f9fafb",
  fontSize: 13, fontFamily: "inherit", boxSizing: "border-box", display: "block",
};

// ------------------------------------------------------------------ //
// Voice panel
// ------------------------------------------------------------------ //
function VoicePanel({ api }) {
  const [sample, setSample] = useState("");
  const [rule, setRule] = useState("");
  const [samples, setSamples] = useState([]);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    api.get("/voice/samples").then((r) => setSamples(r.samples || [])).catch(() => {});
  }, [api]);

  const addSample = async () => {
    if (!sample.trim()) return;
    const res = await api.post("/voice/sample", { text: sample, label: "manual" });
    setSample("");
    setMsg(res.message || "Added.");
    api.get("/voice/samples").then((r) => setSamples(r.samples || []));
  };

  const addRule = async () => {
    if (!rule.trim()) return;
    const res = await api.post("/voice/style-rule", { rule });
    setRule("");
    setMsg(res.message || "Added.");
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      {msg && <div style={{ fontSize: 12, color: "#22c55e", padding: "8px 12px", background: "#14532d22", borderRadius: 6 }}>{msg}</div>}

      <Section title="Writing Samples">
        <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
          Paste examples of your own writing. The agent matches your voice.
        </p>
        <textarea value={sample} onChange={(e) => setSample(e.target.value)}
          placeholder="Paste a paragraph or more of your writing here…"
          rows={8}
          style={{ ...inputStyle, resize: "vertical" }} />
        <Btn onClick={addSample} style={{ marginTop: 8 }}>Add Sample</Btn>
        <div style={{ fontSize: 12, color: "#6b7280", marginTop: 8 }}>{samples.length} sample(s) in memory</div>
      </Section>

      <Section title="Style Rules">
        <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
          Give explicit instructions about how you write.
        </p>
        <input value={rule} onChange={(e) => setRule(e.target.value)}
          placeholder="e.g. I prefer short sentences. I avoid passive voice."
          style={inputStyle} />
        <Btn onClick={addRule} style={{ marginTop: 8 }}>Add Rule</Btn>
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ //
// System panel
// ------------------------------------------------------------------ //
function SystemPanel({ api, onSignOut }) {
  const [proposals, setProposals] = useState([]);
  const [log, setLog] = useState([]);
  const [loading, setLoading] = useState(false);
  const [currentModel, setCurrentModel] = useState("");
  const [models, setModels] = useState([]);
  const [switching, setSwitching] = useState(false);
  const [switchMsg, setSwitchMsg] = useState("");

  useEffect(() => {
    api.get("/improvements/log").then((r) => setLog(r.log || [])).catch(() => {});
    api.get("/settings/model").then((r) => {
      setCurrentModel(r.current || "");
      setModels(r.models || []);
    }).catch(() => {});
  }, [api]);

  const switchModel = async (modelId) => {
    setSwitching(true);
    setSwitchMsg("");
    try {
      const res = await api.post("/settings/model", { model_id: modelId });
      setCurrentModel(res.current);
      setSwitchMsg(`Switched to ${res.current}`);
    } catch (e) {
      setSwitchMsg("Failed to switch model");
    }
    setSwitching(false);
  };

  const loadProposals = async () => {
    setLoading(true);
    const res = await api.get("/improvements/proposals");
    setProposals(res.proposals || []);
    setLoading(false);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>

      <Section title="AI Model">
        <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 14 }}>
          Switch models instantly — takes effect on the next message. No restart needed.
        </p>
        {models.map((m) => (
          <div key={m.id} onClick={() => !switching && switchModel(m.id)}
            style={{
              padding: "10px 14px", borderRadius: 8, marginBottom: 6, cursor: switching ? "default" : "pointer",
              background: currentModel === m.id ? "#1d3a6e" : "#111827",
              border: `1px solid ${currentModel === m.id ? "#1d4ed8" : "#1f2937"}`,
              display: "flex", alignItems: "center", justifyContent: "space-between",
            }}>
            <div>
              <div style={{ fontSize: 13, color: "#f9fafb", fontFamily: "monospace" }}>{m.id}</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>{m.label}</div>
            </div>
            {currentModel === m.id && (
              <div style={{ fontSize: 11, color: "#60a5fa", fontWeight: 600, flexShrink: 0 }}>ACTIVE</div>
            )}
          </div>
        ))}
        <div style={{ marginTop: 10 }}>
          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 6 }}>Or enter a custom model ID:</div>
          <div style={{ display: "flex", gap: 8 }}>
            <input
              id="custom-model-input"
              placeholder="e.g. anthropic/claude-3.5-sonnet"
              style={{ ...inputStyle, flex: 1 }}
              defaultValue=""
            />
            <Btn small onClick={() => {
              const v = document.getElementById("custom-model-input").value.trim();
              if (v) switchModel(v);
            }} disabled={switching}>
              Set
            </Btn>
          </div>
        </div>
        {switchMsg && (
          <div style={{ fontSize: 12, color: "#22c55e", marginTop: 8 }}>{switchMsg}</div>
        )}
      </Section>

      <Section title="Self-Improvement Proposals">
        <Btn onClick={loadProposals} disabled={loading} style={{ marginBottom: 12 }}>
          {loading ? "Analyzing…" : "Generate Improvement Proposals"}
        </Btn>
        {proposals.map((p, i) => (
          <div key={i} style={{ padding: 14, background: "#111827", borderRadius: 8, marginTop: 8, fontSize: 13, color: "#d1d5db" }}>
            <div style={{ fontWeight: 600, color: "#f9fafb", marginBottom: 4 }}>{p.WHAT || p.what || "Proposal"}</div>
            <div style={{ color: "#6b7280" }}>{p.WHY || p.why}</div>
          </div>
        ))}
        {proposals.length === 0 && !loading && (
          <div style={{ fontSize: 13, color: "#6b7280" }}>No proposals yet.</div>
        )}
      </Section>

      <Section title="Event Log">
        {log.length === 0 && <div style={{ fontSize: 13, color: "#6b7280" }}>No events logged yet.</div>}
        {log.slice(-20).reverse().map((e, i) => (
          <div key={i} style={{ fontSize: 12, color: "#6b7280", padding: "6px 0", borderBottom: "1px solid #1f2937" }}>
            <span style={{ color: "#d1d5db" }}>{e.type}</span>
            {" — "}{JSON.stringify(e.details).slice(0, 100)}
          </div>
        ))}
      </Section>

      <Section title="Session">
        <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 12 }}>
          Sign out to enter a different API key.
        </p>
        <Btn onClick={onSignOut} style={{ background: "#374151" }}>Sign Out</Btn>
      </Section>
    </div>
  );
}

// ------------------------------------------------------------------ //
// Shared components
// ------------------------------------------------------------------ //
function StatCard({ label, value }) {
  return (
    <div style={{ padding: "18px 20px", background: "#1f2937", borderRadius: 12 }}>
      <div style={{ fontSize: 26, fontWeight: 700, color: "#f9fafb" }}>{value}</div>
      <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{label}</div>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ padding: 20, background: "#1f2937", borderRadius: 12 }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: "#6b7280", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 14 }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function Btn({ children, onClick, disabled, style = {}, small }) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: small ? "6px 14px" : "9px 18px",
      background: disabled ? "#374151" : "#1d4ed8",
      color: disabled ? "#6b7280" : "#fff",
      border: "none", borderRadius: 8, cursor: disabled ? "default" : "pointer",
      fontSize: small ? 12 : 13, fontWeight: 500, fontFamily: "inherit",
      transition: "background 0.15s", ...style,
    }}>
      {children}
    </button>
  );
}

function EmptyState({ label }) {
  return <div style={{ padding: 32, textAlign: "center", color: "#4b5563", fontSize: 13 }}>{label}</div>;
}

// ------------------------------------------------------------------ //
// Snapshots panel
// ------------------------------------------------------------------ //
function SnapshotsPanel({ api }) {
  const [snapshots, setSnapshots] = useState([]);
  const [viewing, setViewing] = useState(null);
  const [html, setHtml] = useState("");

  useEffect(() => {
    api.get("/snapshots").then((r) => setSnapshots(r.snapshots || [])).catch(() => {});
  }, [api]);

  const view = async (name) => {
    setViewing(name);
    setHtml("Loading...");
    try {
      const res = await fetch(`/api/snapshots/${name}`, {
        headers: { "X-Api-Key": getStoredKey() },
      });
      const text = await res.text();
      setHtml(text);
    } catch (e) {
      setHtml("Failed to load snapshot.");
    }
  };

  return (
    <div>
      <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 16 }}>
        HTML snapshots saved during the last crawl. Use these to inspect what Canvas
        is actually rendering and verify CSS selectors.
      </p>

      {snapshots.length === 0 && <EmptyState label="No snapshots yet. Run a crawl first." />}

      <div style={{ display: "flex", gap: 16 }}>
        <div style={{ width: 280, flexShrink: 0 }}>
          {snapshots.map((s) => (
            <div key={s.name} onClick={() => view(s.name)}
              style={{
                padding: "10px 14px", borderRadius: 8, marginBottom: 6, cursor: "pointer",
                background: viewing === s.name ? "#1d3a6e" : "#1f2937",
                border: `1px solid ${viewing === s.name ? "#1d4ed8" : "transparent"}`,
              }}>
              <div style={{ fontSize: 12, color: "#f9fafb", fontFamily: "monospace", wordBreak: "break-all" }}>
                {s.name}
              </div>
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>{s.size_kb} KB</div>
            </div>
          ))}
        </div>

        {viewing && (
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
              <div style={{ fontSize: 13, color: "#f9fafb", fontFamily: "monospace" }}>{viewing}.html</div>
              <button onClick={() => { setViewing(null); setHtml(""); }}
                style={{ background: "none", border: "none", color: "#6b7280", cursor: "pointer", fontSize: 12 }}>
                Close
              </button>
            </div>
            <iframe
              srcDoc={html}
              style={{
                width: "100%", height: "70vh", border: "1px solid #374151",
                borderRadius: 8, background: "#fff",
              }}
              sandbox="allow-same-origin"
              title={viewing}
            />
          </div>
        )}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------ //
// Root App
// ------------------------------------------------------------------ //
export default function App() {
  const [apiKey, setApiKey] = useState(getStoredKey);
  const [active, setActive] = useState("dashboard");
  const [authError, setAuthError] = useState(false);

  const api = useCallback(() => {
    const a = makeApi(apiKey);
    // Wrap every method to catch AuthError and trigger re-auth
    const wrap = (fn) => async (...args) => {
      try { return await fn(...args); }
      catch (e) {
        if (e instanceof AuthError) { setAuthError(true); throw e; }
        throw e;
      }
    };
    return {
      get: wrap(a.get),
      post: wrap(a.post),
      upload: wrap(a.upload),
      uploadWithFields: wrap(a.uploadWithFields),
    };
  }, [apiKey])();

  const signOut = () => {
    storeKey("");
    setApiKey("");
    setAuthError(false);
  };

  if (!apiKey || authError) {
    return (
      <SetupScreen onSave={(k) => { setApiKey(k); setAuthError(false); }} />
    );
  }

  const panels = {
    dashboard:   <DashboardPanel   api={api} />,
    chat:        <ChatPanel        api={api} />,
    assignments: <AssignmentsPanel api={api} />,
    knowledge:   <KnowledgePanel   api={api} />,
    documents:   <DocumentsPanel   api={api} />,
    voice:       <VoicePanel       api={api} />,
    system:      <SystemPanel      api={api} onSignOut={signOut} />,
    snapshots:   <SnapshotsPanel   api={api} />,
  };

  return (
    <div style={{ display: "flex", minHeight: "100vh", background: "#0d1117", color: "#f9fafb", fontFamily: "'Inter', system-ui, sans-serif" }}>
      <div style={{ width: 200, background: "#111827", display: "flex", flexDirection: "column", padding: "28px 0", flexShrink: 0 }}>
        <div style={{ padding: "0 20px 28px", borderBottom: "1px solid #1f2937" }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#f9fafb", letterSpacing: "-0.01em" }}>Canvas Agent</div>
          <div style={{ fontSize: 11, color: "#4b5563", marginTop: 2 }}>wilmu.instructure.com</div>
        </div>
        <nav style={{ padding: "16px 10px", display: "flex", flexDirection: "column", gap: 2 }}>
          {NAV.map((n) => (
            <button key={n.id} onClick={() => setActive(n.id)} style={{
              display: "flex", alignItems: "center", gap: 10, padding: "8px 12px",
              borderRadius: 8, border: "none", cursor: "pointer", textAlign: "left",
              background: active === n.id ? "#1f2937" : "transparent",
              color: active === n.id ? "#f9fafb" : "#6b7280",
              fontSize: 13, fontFamily: "inherit", fontWeight: active === n.id ? 600 : 400,
              transition: "all 0.1s",
            }}>
              {n.label}
            </button>
          ))}
        </nav>
      </div>

      <div style={{ flex: 1, padding: "32px 40px", overflowY: "auto" }}>
        <div style={{ maxWidth: 860, margin: "0 auto" }}>
          <div style={{ fontSize: 20, fontWeight: 700, marginBottom: 24, letterSpacing: "-0.02em" }}>
            {NAV.find((n) => n.id === active)?.label}
          </div>
          {panels[active]}
        </div>
      </div>
    </div>
  );
}
