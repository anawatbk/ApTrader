# BMAD API & Documentation Guide

Quick reminders for efficient development.

## Two Essential Strategies

### 1. API-First Thinking
Before building any feature, check if an API/library already exists.

### 2. Documentation-First Troubleshooting  
When something doesn't work, immediately check current docs - APIs evolve past your knowledge cutoff.

## Key Checks

**Common patterns with existing solutions:**
- Git operations → `gh` CLI or GitHub API
- Cloud services → Official SDKs
- Auth/validation/parsing → Established libraries
- File operations → Cloud storage SDKs

**Frequently evolved services:**
- Cloudflare (Workers, KV, R2, D1)
- Supabase (Auth, Realtime, Vectors)
- Next.js (App Router, Server Components)
- AI APIs (OpenAI, Anthropic)

**When errors occur:**
1. Check current docs (not your memorized version)
2. Look for breaking changes
3. Test with doc examples
4. Only then build workarounds