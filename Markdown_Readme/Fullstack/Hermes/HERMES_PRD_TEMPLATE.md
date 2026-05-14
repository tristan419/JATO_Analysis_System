# PRD: [Feature Title]

> Template version: 1.0
> Hermes Phase 2 — PRD Intake compatible
> Copy this file and fill in all sections marked with `[ ]`.

---

## 1. Background

[Describe the problem or opportunity. Why does this feature need to exist?]

---

## 2. Goal

[What does success look like? One or two sentences.]

---

## 3. Scope

[What is in scope? What is explicitly out of scope?]

---

## 4. User Flow

[Describe the user journey. Screenshots or ASCII diagrams welcome.]

---

## 5. Data / API Requirements

- **New API endpoints:** [list or "none"]
- **Modified API endpoints:** [list or "none"]
- **New database tables:** [list or "none"]
- **Modified database schema:** [list or "none"]
- **Data sources:** [list — e.g. JATO parquet, PostgreSQL table, VOC artifact]

---

## 6. Frontend Requirements

- **New routes:** [list or "none"]
- **New components:** [list or "none"]
- **Modified pages:** [list or "none"]
- **New dependencies:** [list or "none"]

---

## 7. Backend Requirements

- **New services:** [list or "none"]
- **Modified services:** [list or "none"]
- **New environment variables:** [list or "none"]
- **New dependencies:** [list or "none"]

---

## 8. Pipeline / Crawler / Artifact Impact

- **Affected crawler:** [list or "none"]
- **Affected Airflow DAG:** [list or "none"]
- **Affected systemd timer:** [list or "none"]
- **Affected GitHub Action:** [list or "none"]
- **New or modified artifact:** [list or "none"]

---

## 9. LLM / Prompt / Evidence Impact

- **Affected prompt:** [list or "none"]
- **Affected model route (Flash/Pro):** [Flash / Pro / both / none]
- **Evidence required:** [list or "none"]
- **Cost risk:** [low / medium / high]
- **New prompt version required:** [yes / no]

---

## 10. Hermes Governance

### Feature Registry

- **Feature ID:** `feature.[name]`
- **Existing feature or new feature:** [existing / new]
- **Affected routes:** [list]
- **Affected APIs:** [list]
- **Affected data sources:** [list]

### Pipeline Impact

- **Affected crawler:** [list]
- **Affected Airflow DAG:** [list]
- **Affected systemd timer:** [list]
- **Affected GitHub Action:** [list]
- **Affected artifact:** [list]

### Intelligence Impact

- **Affected prompt:** [list]
- **Affected model route:** [Flash / Pro / both / none]
- **Evidence required:** [list]
- **Flash / Pro usage:** [Flash / Pro / both]
- **Cost risk:** [low / medium / high]

### Code Audit Requirements

- **Contract test required:** [yes / no]
- **Frontend type update:** [yes / no]
- **Backend schema change:** [yes / no]
- **Env var change:** [yes / no]
- **Docs update:** [yes / no]

---

## 11. Acceptance Criteria

- [ ] Criterion 1
- [ ] Criterion 2
- [ ] Criterion 3
