# Workflow Docs

This folder contains workflow maps for the current JATO analysis system from two angles:

1. **Business presentation / deck view**
   - `BUSINESS_PRESENTATION_DECK.md`
2. **Business-facing pipeline view**
   - `BUSINESS_PIPELINE_WORKFLOWS.md`
3. **Technical/system view**
   - `REPOSITORY_SYSTEM_WORKFLOW.md`

Recommended reading order:

1. `BUSINESS_PRESENTATION_DECK.md`
2. `BUSINESS_PIPELINE_WORKFLOWS.md`
3. `REPOSITORY_SYSTEM_WORKFLOW.md`

The business document answers:

- How JATO monthly import becomes usable analysis data
- How MSRP scraping becomes pricing intelligence
- How market scan / positioning / competitor monitoring are implemented on the current pipeline
- How country scan / Copilot are grounded by data + news + prices

The presentation/deck document answers:

- What the business actually wants to achieve
- Which business workflows matter most
- How those workflows map onto the current JATO pipeline
- How to explain the system in a business-review or stakeholder presentation

The technical document answers:

- How the repository is split into data, scripts, scraping, orchestration, application, legacy dashboard, and GitNexus
- Where the important scripts, DAGs, data outputs, and app endpoints fit together
- How Country Copilot currently routes/scopes questions and how its new planner-first architecture is moving toward a top-tier internal knowledge assistant pattern
