# BMAD Session Continuity Guide

This guide ensures seamless multi-session workflows with proper context preservation.

## Session Intelligence

### Context Preservation
When starting any session, systematically check for existing context:

1. **Read session state**: Check `docs/.bmad-session/current-state.md` for:
   - `active_persona`: Which persona was last active
   - `current_task`: Specific task being executed
   - `workflow_position`: Where in the persona's process we stopped
   - `pending_handoffs`: Any planned persona transitions

2. **Resume persona execution**:
   - Load the `active_persona` file completely
   - Continue from `workflow_position` using `current_task` context
   - Apply persona's perspective to `next_priority` items

3. **Handle incomplete handoffs**:
   - If `pending_handoffs` exist, complete the transition
   - Provide transition context from previous persona's work
   - Apply new persona's perspective to continue workflow

### Enhanced Session State Requirements
The session state template should capture:

```yaml
# Required for continuity
active_persona: architect
current_task: create-architecture
workflow_position: "designing API contracts"
pending_handoffs:
  - to: designer
    context: "system architecture complete, need UX alignment"
    deliverables: ["architecture.md", "api-specification.md"]
```

### Session Resumption Algorithm
```
Check session state exists?
├─ YES: Resume execution
│   ├─ Load active_persona file
│   ├─ Apply workflow_position context  
│   ├─ Continue current_task
│   └─ Process pending_handoffs
└─ NO: Route based on intent recognition
```

### Multi-Session Workflow Continuity
For complex multi-persona workflows:

1. **Track coordination state**: Which personas have completed their contributions
2. **Maintain handoff queue**: Ordered list of pending transitions
3. **Preserve work context**: Link between persona outputs and next steps
4. **Resume coordination**: Pick up orchestrator workflows mid-process

### Session State Maintenance
Throughout execution, maintain session continuity:

1. **Update active state**: When starting new tasks or changing workflow position
2. **Document pending handoffs**: When persona work approaches completion
3. **Log key decisions**: For context preservation across sessions
4. **Track deliverables**: Link completed artifacts to next workflow steps

**Example state updates:**
```
# Starting new task
active_persona: pm
current_task: create-next-story
workflow_position: "defining Epic 2 acceptance criteria"

# Approaching handoff
pending_handoffs:
  - to: developer
    context: "Story 2.1 complete with acceptance criteria"
    deliverables: ["story-2-1.md"]
    next_action: "implement-story"
```

### Adaptive Complexity
- **Simple requests**: Single persona may be sufficient
- **Complex features**: Plan multi-persona coordination via **Orchestrator**
- **Uncertain scope**: Start with **Analyst** investigation
- **Session resumption**: Continue from documented state