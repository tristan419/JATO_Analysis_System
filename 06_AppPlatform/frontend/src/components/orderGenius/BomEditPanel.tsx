import type { FormEventHandler, ReactNode } from "react";

import "./BomEditPanel.css";

export type BomEditCountryOption = {
  code: string;
  hasFob: boolean;
  selected: boolean;
  title: string;
  onToggle: (checked: boolean) => void;
};

export type BomEditSaveMessage = {
  kind: "success" | "error";
  text: string;
};

type BomEditPanelProps = {
  onSubmit: FormEventHandler<HTMLFormElement>;
  productFields: ReactNode;
  lifecycle: ReactNode;
  countries: BomEditCountryOption[];
  selectedCountryCount: number;
  fobDeltaEur: string;
  fobSaving: boolean;
  fobError?: string | null;
  onFobDeltaChange: (value: string) => void;
  onApplyFobDelta: () => void;
  onApplyQuickFobDelta: (deltaEur: number) => void;
  onSelectFilledCountries: () => void;
  onSelectUnfilledCountries: () => void;
  onSelectAllCountries: () => void;
  onClearCountries: () => void;
  noteKey: string;
  noteDefaultValue: string;
  saveMessage?: BomEditSaveMessage | null;
  onCopyMaterial: () => void;
  isSavingProduct: boolean;
};

export function BomEditPanel({
  onSubmit,
  productFields,
  lifecycle,
  countries,
  selectedCountryCount,
  fobDeltaEur,
  fobSaving,
  fobError,
  onFobDeltaChange,
  onApplyFobDelta,
  onApplyQuickFobDelta,
  onSelectFilledCountries,
  onSelectUnfilledCountries,
  onSelectAllCountries,
  onClearCountries,
  noteKey,
  noteDefaultValue,
  saveMessage,
  onCopyMaterial,
  isSavingProduct,
}: BomEditPanelProps) {
  return (
    <form onSubmit={onSubmit} className="bom-edit-product-form">
      <div className="bom-edit-main-row">
        <div className="bom-edit-product-card">
          <span className="bom-edit-card-title">Product fields</span>
          <div className="bom-edit-product-controls">{productFields}</div>
        </div>
        <div className="bom-edit-lifecycle-card">
          <span className="bom-edit-card-title">Lifecycle</span>
          {lifecycle}
        </div>
      </div>

      <div className="bom-edit-ops-panel">
        <CountryChipList
          countries={countries}
          selectedCountryCount={selectedCountryCount}
        />
        <FobToolsBar
          deltaEur={fobDeltaEur}
          saving={fobSaving}
          selectedCountryCount={selectedCountryCount}
          onDeltaChange={onFobDeltaChange}
          onApply={onApplyFobDelta}
          onQuickApply={onApplyQuickFobDelta}
          onSelectFilled={onSelectFilledCountries}
          onSelectUnfilled={onSelectUnfilledCountries}
          onSelectAll={onSelectAllCountries}
          onClear={onClearCountries}
        />
        {fobError ? <div className="bom-edit-error-message">{fobError}</div> : null}
      </div>

      <div className="bom-edit-note-row">
        <span className="bom-edit-card-title">Note</span>
        <textarea
          key={noteKey}
          name="remark"
          defaultValue={noteDefaultValue}
          placeholder="What changed on this material compared with previous version..."
          rows={2}
          className="bom-edit-note-input"
        />
        <div className="bom-edit-action-row">
          {saveMessage ? (
            <span className={`bom-edit-save-message is-${saveMessage.kind}`}>
              {saveMessage.text}
            </span>
          ) : null}
          <button
            type="button"
            className="btn btn-sm btn-ghost"
            onClick={onCopyMaterial}
          >
            Copy Material
          </button>
          <button
            type="submit"
            className="btn btn-sm btn-primary"
            disabled={isSavingProduct}
          >
            {isSavingProduct ? "Saving..." : "Save Changes"}
          </button>
        </div>
      </div>
    </form>
  );
}

function CountryChipList({
  countries,
  selectedCountryCount,
}: {
  countries: BomEditCountryOption[];
  selectedCountryCount: number;
}) {
  return (
    <details className="bom-edit-country-details" open>
      <summary className="bom-edit-country-summary">
        <span className="bom-edit-country-caret" aria-hidden="true" />
        <span>Selected countries</span>
        <strong>{selectedCountryCount}</strong>
      </summary>
      <div className="bom-edit-country-chips">
        {countries.map((country) => (
          <label
            key={`bom-edit-country-${country.code}`}
            className={`bom-edit-country-chip${country.hasFob ? " has-fob" : " is-empty"}`}
            title={country.title}
          >
            <input
              type="checkbox"
              checked={country.selected}
              onChange={(event) => country.onToggle(event.target.checked)}
            />
            <span>{country.code}</span>
          </label>
        ))}
      </div>
    </details>
  );
}

function FobToolsBar({
  deltaEur,
  saving,
  selectedCountryCount,
  onDeltaChange,
  onApply,
  onQuickApply,
  onSelectFilled,
  onSelectUnfilled,
  onSelectAll,
  onClear,
}: {
  deltaEur: string;
  saving: boolean;
  selectedCountryCount: number;
  onDeltaChange: (value: string) => void;
  onApply: () => void;
  onQuickApply: (deltaEur: number) => void;
  onSelectFilled: () => void;
  onSelectUnfilled: () => void;
  onSelectAll: () => void;
  onClear: () => void;
}) {
  return (
    <div className="bom-edit-fob-tools">
      <span className="bom-edit-card-title bom-edit-ops-title">FOB tools</span>
      <div className="bom-edit-fob-controls">
        <input
          type="number"
          value={deltaEur}
          placeholder="± EUR"
          onChange={(event) => onDeltaChange(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") event.preventDefault();
          }}
          className="bom-edit-fob-delta-input"
        />
        <button
          type="button"
          className="btn btn-sm btn-primary"
          disabled={saving}
          onClick={onApply}
        >
          {saving ? "Saving..." : `Apply to ${selectedCountryCount || 0}`}
        </button>
        <button
          type="button"
          className="btn btn-sm btn-ghost bom-edit-fob-quick-button is-positive"
          disabled={saving}
          onClick={() => onQuickApply(200)}
        >
          +200
        </button>
        <button
          type="button"
          className="btn btn-sm btn-ghost bom-edit-fob-quick-button is-negative"
          disabled={saving}
          onClick={() => onQuickApply(-300)}
        >
          -300
        </button>
        <button
          type="button"
          className="btn btn-sm btn-ghost"
          title="Click: countries with FOB. Double-click: countries without FOB."
          onClick={onSelectFilled}
          onDoubleClick={onSelectUnfilled}
        >
          Filled
        </button>
        <button
          type="button"
          className="btn btn-sm btn-ghost"
          title="Select every visible country column"
          onClick={onSelectAll}
        >
          All
        </button>
        <button
          type="button"
          className="btn btn-sm btn-ghost"
          title="Clear selected countries"
          onClick={onClear}
        >
          Clear
        </button>
      </div>
    </div>
  );
}
