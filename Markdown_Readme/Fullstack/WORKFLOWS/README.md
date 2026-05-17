# Workflow Docs

This folder contains workflow maps for the current JATO analysis system from three angles:

1. **Business presentation / deck view** — `BUSINESS_PRESENTATION_DECK.md`
   - What the business wants to achieve
   - Which business workflows matter most
   - How to explain the system in a stakeholder presentation

2. **Business-facing pipeline view** — `BUSINESS_PIPELINE_WORKFLOWS.md`
   - How JATO monthly import becomes usable analysis data
   - How MSRP scraping becomes pricing intelligence
   - How market scan / positioning / competitor monitoring are implemented
   - How country scan / Copilot are grounded by data + news + prices
   - How engineering config management works (parse → match → diff → publish)

3. **Technical/system view** — `REPOSITORY_SYSTEM_WORKFLOW.md`
   - Full Mermaid diagrams: business/data chain, AppPlatform runtime, GitNexus chain
   - Where scripts, DAGs, data outputs, and app endpoints fit together
   - Country Copilot routing architecture (current + target + planner-first)

4. **Diagram index** — `DIAGRAMS_INDEX.md`
   - Central index of all Mermaid diagrams across documentation
   - Cross-reference to source documents for each diagram

## Recommended reading order

1. `BUSINESS_PRESENTATION_DECK.md` — business context first
2. `BUSINESS_PIPELINE_WORKFLOWS.md` — pipeline details
3. `REPOSITORY_SYSTEM_WORKFLOW.md` — technical deep-dive
4. `DIAGRAMS_INDEX.md` — visual reference
