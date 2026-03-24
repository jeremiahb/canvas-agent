# Canvas AI Student Agent — Setup Guide

## What you're deploying

A fully autonomous AI student agent that:
- Logs into Canvas (wilmu.instructure.com) using your browser session
- Crawls all your courses, assignments, rubrics, announcements, files, and grades
- Downloads and reads all attached PDFs, Word docs, and linked documents
- Generates Word, PowerPoint, and Excel assignments in your voice
- Lets you review and approve everything before submission
- Runs 24/7 on a cloud server with a persistent knowledge base

---

## Before you start — what you need

| Item | Where to get it |
|---|---|
| Python 3.10+ on Windows | python.org/downloads (check "Add to PATH") |
| Node.js 18+ on Windows | nodejs.org |
| Git on Windows | git-scm.com |
| GitHub account (free) | github.com |
| Railway account (free) | railway.app |
| AI API key | See Step 2 below |

---

## Step 1 — Choose your AI model and get an API key

The agent needs an AI model. You have two options:

**Option A — OpenRouter (recommended to start, very low cost)**
- Sign up at openrouter.ai
- Go to Keys → Create Key
- Copy the key — you'll use it in Step 5
- Free tier available. Paid use costs ~$1-2/month for typical use.
- Set `AI_MODEL` to: `meta-llama/llama-3.3-70b-instruct:free`

**Option B — Anthropic Claude (best quality)**
- Sign up at console.anthropic.com
- Go to API Keys → Create Key
- Copy the key — you'll use it in Step 5
- Costs ~$10-18/month depending on chat volume
- Set `AI_MODEL` to: `claude-sonnet-4-20250514`

You can switch between them at any time by changing two variables in Railway.

---

## Step 2 — Generate your API secret key

The dashboard is protected by a secret key that only you know.
Generate one now and save it somewhere safe — you'll need it in Step 5 and when you first open the dashboard.

Run this in any terminal (works on Windows Command Prompt):
```
python -c "import secrets; print(secrets.token_hex(32))"
```

You'll get something like:
```
a3f8c2d1e4b7f9a0c2e4f6a8b0c2d4e6f8a0b2c4d6e8f0a2b4c6d8e0f2a4b6
```

Save this string. This is your `API_SECRET`.

---

## Step 3 — Export your Canvas cookies

The agent logs into Canvas using your browser session. You export the cookies once and upload them to the server.

**1. Log into Canvas in Chrome**
- Go to https://wilmu.instructure.com
- Sign in with your Microsoft credentials
- Confirm your courses appear on the dashboard

**2. Close Chrome completely**
- Close all Chrome windows
- Check the system tray (bottom-right) for a Chrome icon — right-click → Exit

**3. Install the cookie exporter (first time only)**
```
pip install browser-cookie3
```

**4. Run the exporter from your canvas-agent folder**
```
python scripts/export_cookies.py
```

Output:
```
[OK] Exported 47 cookies successfully!
Saved to: C:\...\canvas-agent\canvas_cookies.json
```

> **Alternative if the script fails:** Install the "Cookie-Editor" Chrome extension,
> log into Canvas, click the extension, click Export → JSON, save as canvas_cookies.json.

---

## Step 4 — Push to GitHub

**1. Create a new private GitHub repository**
- Go to github.com/new
- Name it `canvas-agent`, set to Private
- Do NOT initialize with README
- Click Create Repository

**2. Configure Git (first time only)**
```
git config --global user.email "you@email.com"
git config --global user.name "Your Name"
```

**3. Push from your canvas-agent folder**
```
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/canvas-agent.git
git push -u origin main
```

---

## Step 5 — Deploy to Railway

**1. Create a Railway project**
- Go to railway.app and sign in
- Click New Project → Deploy from GitHub repo
- Connect your GitHub account if prompted
- Select your canvas-agent repository → Deploy Now

**2. Get your public URL**
- Click your service → Settings → Domains → Generate Domain
- Copy the URL (e.g. `https://canvas-agent-production.up.railway.app`)

**3. Add a persistent volume**

Without this, all data is lost on every restart.

- Click your service → Volumes tab → + Add Volume
- Set Mount Path to: `/data`
- Click Add

**4. Add environment variables**

Click your service → Variables tab → + New Variable for each:

| Variable | Value |
|---|---|
| `API_SECRET` | The secret you generated in Step 2 |
| `ANTHROPIC_API_KEY` | Your key from console.anthropic.com (if using Claude) |
| `OPENROUTER_API_KEY` | Your key from openrouter.ai (if using OpenRouter) |
| `AI_MODEL` | `meta-llama/llama-3.3-70b-instruct:free` or `claude-sonnet-4-20250514` |
| `ALLOWED_ORIGINS` | Your Railway URL — no trailing slash |
| `CANVAS_URL` | `https://wilmu.instructure.com` |
| `DATA_DIR` | `/data` |

**5. Redeploy**
- Click the Deployments tab → Redeploy
- First build takes 3-5 minutes (installs Chrome and all dependencies)
- When you see `Application started` in the logs, it's live

---

## Step 6 — Connect the dashboard

**1. Open your Railway URL in any browser**

**2. Enter your API secret key**
- The dashboard shows a Connect screen on first load
- Paste the API_SECRET you generated in Step 2
- Click Connect — it verifies the key against your server

**3. Upload your cookies**
- Dashboard tab → Canvas Session → Choose File
- Select your `canvas_cookies.json`
- Click Upload

**4. Run the first crawl**
- Click Crawl Canvas Now
- The agent logs into Canvas and reads every course, assignment, rubric,
  announcement, module, syllabus, grade, and all attached files
- Takes 5-15 minutes depending on how many courses and files you have
- When finished you'll see "Crawl complete — N documents indexed"

**5. Check for flagged external links**
- Go to the Documents tab
- Flagged items are readings on platforms like VitalSource or Pearson
  that the agent found but couldn't access automatically
- Use Paste Text or Upload File to add them manually

---

## Step 7 — Train your voice

Before generating anything, give the agent writing samples:

**Voice & Style → Writing Samples**
- Paste 3-4 paragraphs of your own past writing
- Essays, discussion posts, anything academic works

**Voice & Style → Style Rules**
- Add one explicit rule at a time
- Examples: "I write in first person", "I avoid passive voice", "I prefer short sentences"

---

## Step 8 — Generate your first assignment

1. Assignments tab → find the assignment
2. Click Analyze — reads the full rubric, shows confidence score
3. Click Generate — drafts the document in your voice (30-90 seconds)
4. Click Download — open and review the file
5. Click Approve or Revise with feedback

Nothing is ever submitted to Canvas automatically. You submit the file yourself.

---

## Updating cookies when they expire

Sessions last 1-4 weeks. When you see "Session invalid" in the dashboard:

1. Log into Canvas in Chrome
2. Close Chrome completely
3. Run: `python scripts/export_cookies.py`
4. Dashboard → Upload the new canvas_cookies.json

---

## Switching AI models

Change two variables in Railway → Variables:
- `AI_MODEL` — the new model string
- `ANTHROPIC_API_KEY` or `OPENROUTER_API_KEY` — whichever the new model uses

Then redeploy. No code changes needed.

---

## Updating the agent

When new features are added, push to GitHub and Railway redeploys automatically:
```
git add .
git commit -m "Update"
git push
```

Your data on the volume (/data) is never affected by redeployments.

---

## Environment variables reference

| Variable | Required | Description |
|---|---|---|
| `API_SECRET` | Yes | Dashboard authentication key. Generate with `python -c "import secrets; print(secrets.token_hex(32))"` |
| `ANTHROPIC_API_KEY` | If using Claude | Key from console.anthropic.com |
| `OPENROUTER_API_KEY` | If using OpenRouter | Key from openrouter.ai |
| `AI_MODEL` | Yes | `meta-llama/llama-3.3-70b-instruct:free` or `claude-sonnet-4-20250514` |
| `ALLOWED_ORIGINS` | Yes | Your Railway URL, no trailing slash |
| `CANVAS_URL` | Yes | `https://wilmu.instructure.com` |
| `DATA_DIR` | Yes | `/data` — must match your volume mount path |
| `PORT` | Auto | Set by Railway automatically |

---

## Project structure

```
canvas-agent/
├── scripts/
│   └── export_cookies.py       # Run locally to export Chrome cookies
├── agent/
│   ├── crawler.py              # Playwright Canvas navigator
│   ├── document_ingester.py    # PDF/Word/embedded doc reader
│   ├── knowledge_base.py       # ChromaDB vector store
│   ├── brain.py                # AI-powered intelligence
│   └── file_generator.py       # docx/pptx/xlsx generator
├── api/
│   └── main.py                 # FastAPI backend
├── dashboard/
│   └── src/App.jsx             # React dashboard UI
├── data/                       # Created at runtime on the volume
│   ├── cookies/                # Canvas session
│   ├── knowledge/              # ChromaDB + crawl data
│   ├── assignments/            # Generated files
│   ├── queue.json              # Draft queue
│   ├── crawl_status.json       # Last crawl status
│   └── improvement_log.json    # Self-improvement history
├── requirements.txt
├── railway.toml
└── nixpacks.toml
```
