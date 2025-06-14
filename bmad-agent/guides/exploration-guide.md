# BMAD Exploration Guide

This guide enables creative exploration while capturing insights for later production use.

## Adaptive Formality

You may adapt BMAD's rigor based on detected context and intent to enable creative exploration while still capturing insights.

### Automatic Adaptation
When you detect exploration/prototyping context with high confidence (≥80%), automatically adapt to lighter-weight processes and notify the user:

> 🔬 **Switching to exploration mode** - Using lighter documentation and deferred checklists for rapid prototyping. To return to full BMAD rigor, just say "let's build this properly" or "production mode."

### Context Indicators for Exploration Mode
- "Let me try something"
- "I want to experiment with"
- "Just exploring an idea"
- "Quick prototype"
- "See if this works"
- Creative brainstorming language

### Exploration Mode Means
- **Templates become flexible guides** - fill in discovered sections as you go
- **Progressive template population** - capture specific discoveries directly in draft templates
- **Planning journal for meta-decisions** - track why choices were made
- **Checklists deferred** until approach validated
- **Focus on learning** over process compliance
- **Rapid iteration** encouraged
- **Creative solutions** prioritized over structure

### Smart Exploration Documentation
During exploration, progressively build formal artifacts:

1. **Create draft templates** with discovered content:
   - `docs/drafts/prd-exploration.md` - Capture features, users, requirements as discovered
   - `docs/drafts/architecture-exploration.md` - Document technical decisions as made
   - Mark sections with `[EXPLORED]` or `[TODO]` tags

2. **Use planning journal** for:
   - Why decisions were made
   - What alternatives were considered
   - Key insights and breakthroughs
   - Pivot points and direction changes

3. **Transition to production**:
   - Review and refine draft templates
   - Run validation checklists
   - Fill in missing sections
   - Move from `drafts/` to main `docs/`

### Production Mode Means
- **Full BMAD process** applies
- **All templates** completed thoroughly
- **All checklists** must pass
- **Complete documentation** required
- **Quality gates** enforced
- **Systematic validation** throughout

### Mode Switching Commands
**To exploration mode**: 
- "Let's explore this idea"
- "I want to experiment"
- "Quick and dirty prototype"

**To production mode**:
- "Let's build this properly"
- "Make this production ready"
- "Apply full rigor"

### Why This Matters
Exploration mode enables:
- **Creative discovery** without process overhead
- **Rapid prototyping** to validate concepts
- **Learning by doing** rather than planning
- **Captured insights** that inform later production work

The key is maintaining the planning journal even in exploration mode, ensuring valuable discoveries aren't lost when transitioning to production development.

## Exploration Mode Persona Identifiers

When in exploration mode, append `-Explore` to persona identifiers:
- `[Analyst-Explore]` - Investigating possibilities without constraints
- `[PM-Explore]` - Rapidly capturing feature ideas
- `[Architect-Explore]` - Experimenting with technical approaches
- `[Designer-Explore]` - Creative UI/UX experimentation
- `[Developer-Explore]` - Prototyping implementations

Mode transitions should be announced clearly:
- "🔬 Switching to exploration mode as [PM-Explore]..."
- "📋 Returning to production mode as [PM]..."