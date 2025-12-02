# AI-Enabled PLC Code System for CODESYS

A chat-based web application that enables AI-assisted PLC code generation and modification for CODESYS projects, using the existing export/import/diff tooling as the foundation.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Web Frontend (React)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Chat Panel   │  │ Project Tree │  │ Code/Diff Viewer │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Backend API (FastAPI)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Anthropic AI │  │ Session Mgmt │  │ CODESYS Tools    │  │
│  │ Integration  │  │              │  │ (export/import)  │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Phase 1: Backend Foundation

### 1.1 Project Management API
Create a FastAPI backend that:
- Manages "sessions" (working copies of exported `.st` files)
- Exposes endpoints: `/projects`, `/projects/{id}/files`, `/projects/{id}/chat`
- Integrates existing scripts as Python modules

Key files to create:
- `server/main.py` - FastAPI app entry point
- `server/projects.py` - Project/session management
- `server/codesys_tools.py` - Wrapper around existing scripts

### 1.2 Anthropic Integration
- Claude API for code generation (using claude-sonnet-4-20250514)
- System prompt with Structured Text expertise
- Tool definitions allowing AI to:
  - Read project files
  - Propose file modifications (as diffs)
  - Create new POUs/GVLs
- Context: Include relevant project files in each request

Key file: `server/ai_agent.py`

## Phase 2: Web Frontend

### 2.1 Core UI Components
- **Chat panel**: Message history, input box, streaming responses
- **Project tree**: Collapsible view of POUs, GVLs, methods
- **Code viewer**: Syntax-highlighted ST code with diff highlighting
- **Apply button**: Commit proposed changes to working files

Tech: React + TypeScript, TailwindCSS, syntax highlighting library

Key files:
- `frontend/src/App.tsx` - Main layout
- `frontend/src/components/Chat.tsx`
- `frontend/src/components/ProjectTree.tsx`
- `frontend/src/components/CodeViewer.tsx`

## Phase 3: AI Capabilities

### 3.1 Tool Definitions for Claude

```python
tools = [
    {
        "name": "read_file",
        "description": "Read a POU, GVL, or method file from the project",
        "input_schema": {"file_path": "string"}
    },
    {
        "name": "propose_edit",
        "description": "Propose changes to an existing file (returns diff)",
        "input_schema": {"file_path": "string", "new_content": "string"}
    },
    {
        "name": "create_file", 
        "description": "Create a new POU, GVL, or method",
        "input_schema": {"file_name": "string", "content": "string"}
    },
    {
        "name": "list_project_files",
        "description": "List all files in the project"
    }
]
```

### 3.2 System Prompt
Include:
- Structured Text syntax reference
- CODESYS conventions (VAR_GLOBAL, PROGRAM/FUNCTION_BLOCK, etc.)
- Project context (file list + selected file contents)
- Output format instructions (propose changes as complete file contents)

## Implementation Strategy

**Start minimal**: CLI-based proof of concept first, then add web UI.

Files to create:
1. `server/requirements.txt` - anthropic, fastapi, uvicorn
2. `server/main.py` - API endpoints
3. `server/ai_agent.py` - Claude integration with tools
4. `server/codesys_tools.py` - Thin wrapper over existing scripts
5. `frontend/` - React app (Phase 2)

## First Milestone: CLI Chat Agent

Before building the web UI, create a working CLI tool:

```bash
python ai_plc_chat.py --project ./tests/untitled1_export/
> Add a counter variable to GVL and increment it in PLC_PRG
[AI proposes changes, shows diff]
> approve
[Changes applied]
```

This validates the AI integration before investing in UI work.

## Implementation Todos

- [ ] Create CLI chat agent with Anthropic API integration and tool definitions
- [ ] Create project wrapper module that uses existing export/diff/apply scripts
- [ ] Build FastAPI backend with project management and chat endpoints
- [ ] Build React frontend with chat panel, project tree, and code viewer

