import { useEffect, useMemo, useRef, useState } from "react";
import { animate } from "animejs";

import type {
  CountryMaterialFinanceImportPreview,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";
import { LoopingCountStrip } from "../LoopingCountStrip";
import { RollingTickerCard } from "../RollingTickerCard";
import { formatCountryCodeTooltip } from "../../utils/jatoCountries";
import { CountryCbuPastePanel } from "./CountryCbuPastePanel";
import { MaterialFinanceMatrix } from "./MaterialFinanceMatrix";

interface MaterialFinanceWorkbenchProps {
  countryCode: string;
  countryCodes: string[];
  rows: CountryMaterialFinanceRow[];
  loading: boolean;
  error?: string;
  savingMaterialCode?: string | null;
  onCountryChange: (countryCode: string) => void | Promise<void>;
  onPreviewImport?: (
    countryCode: string,
    payload: { file?: File; text?: string },
  ) => Promise<CountryMaterialFinanceImportPreview>;
  onViewHistory?: (row: CountryMaterialFinanceRow) => void | Promise<void>;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

function hasFinanceValue(row: CountryMaterialFinanceRow): boolean {
  return [
    row.fobEur,
    row.vehicleMarginEur,
    row.vehicleMarginRate,
    row.vehicleProfitEur,
    row.vehicleProfitRate,
    row.fobDeltaEur,
    row.marginDeltaEur,
  ].some((value) => value != null) || Boolean(row.memo?.trim());
}

export function MaterialFinanceWorkbench({
  countryCode,
  countryCodes,
  rows,
  loading,
  error = "",
  savingMaterialCode = null,
  onCountryChange,
  onPreviewImport,
  onViewHistory,
  onSaveRow,
}: MaterialFinanceWorkbenchProps) {
  const isNlPriceCbu = countryCode === "NL";
  const countryStripRef = useRef<HTMLDivElement | null>(null);
  const dockFrameRef = useRef<number | null>(null);
  const [dockLabel, setDockLabel] = useState<{ text: string; left: number; top: number } | null>(null);
  const filledRows = useMemo(
    () => rows.filter(hasFinanceValue).length,
    [rows],
  );
  const tickerItems = useMemo(
    () => rows.map((row) => `${row.materialCode} · ${row.modelName}`),
    [rows],
  );

  useEffect(() => () => {
    if (dockFrameRef.current != null) {
      window.cancelAnimationFrame(dockFrameRef.current);
    }
  }, []);

  const setCountryDockLabel = (code: string, chip: HTMLButtonElement) => {
    const rect = chip.getBoundingClientRect();
    setDockLabel({
      text: formatCountryCodeTooltip(code),
      left: rect.left + rect.width / 2,
      top: Math.max(8, rect.top - 34),
    });
  };

  const getCountryChipFromPointer = (pointerX: number, target: EventTarget | null): HTMLButtonElement | null => {
    if (target instanceof HTMLElement) {
      const directChip = target.closest<HTMLButtonElement>(".material-finance-country-chip");
      if (directChip) return directChip;
    }

    const strip = countryStripRef.current;
    if (!strip) return null;

    let nearestChip: HTMLButtonElement | null = null;
    let nearestDistance = Number.POSITIVE_INFINITY;
    const chips = Array.from(strip.querySelectorAll<HTMLButtonElement>(".material-finance-country-chip"));
    chips.forEach((chip) => {
      const rect = chip.getBoundingClientRect();
      const distance = Math.abs(pointerX - (rect.left + rect.width / 2));
      if (distance < nearestDistance) {
        nearestChip = chip;
        nearestDistance = distance;
      }
    });
    return nearestDistance <= 28 ? nearestChip : null;
  };

  const animateCountryDock = (pointerX: number | null) => {
    if (dockFrameRef.current != null) {
      window.cancelAnimationFrame(dockFrameRef.current);
    }

    dockFrameRef.current = window.requestAnimationFrame(() => {
      const strip = countryStripRef.current;
      if (!strip) return;

      const chips = Array.from(strip.querySelectorAll<HTMLButtonElement>(".material-finance-country-chip"));
      chips.forEach((chip) => {
        if (pointerX == null) {
          chip.style.zIndex = "";
          animate(chip, {
            scale: 1,
            translateY: 0,
            width: "34px",
            minWidth: "34px",
            opacity: 1,
            duration: 120,
            ease: "outQuad",
          });
          return;
        }

        const rect = chip.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        const distance = Math.abs(pointerX - centerX);
        const influence = Math.max(0, 1 - distance / 86);
        const eased = 0.5 - Math.cos(influence * Math.PI) / 2;
        const scale = 1 + eased * 0.08;
        const width = 34 + eased * 14;
        const translateY = -11 * eased;
        chip.style.zIndex = String(Math.round(influence * 10));
        animate(chip, {
          scale,
          translateY,
          width: `${width}px`,
          minWidth: `${width}px`,
          opacity: 1,
          duration: 120,
          ease: "outQuad",
        });
      });
    });
  };

  return (
    <section className="material-finance-workbench">
      <header className="material-finance-workbench-head">
        <div className="review-table-toolbar-status material-finance-workbench-reel">
          <LoopingCountStrip
            current={filledRows}
            total={rows.length}
            label="Filled / Rows"
            meta={countryCode}
            pauseMs={1800}
          />
          <RollingTickerCard
            title="CBU BOMs"
            items={tickerItems}
            emptyLabel="No CBU rows"
            pauseMs={1100}
            variant="reel-only"
          />
        </div>
        <div
          ref={countryStripRef}
          className="material-finance-country-strip"
          aria-label="CBU country"
          onPointerMove={(event) => {
            animateCountryDock(event.clientX);
            const chip = getCountryChipFromPointer(event.clientX, event.target);
            const chipCountryCode = chip?.dataset.countryCode;
            if (chip && chipCountryCode) {
              setCountryDockLabel(chipCountryCode, chip);
            }
          }}
          onPointerLeave={() => {
            setDockLabel(null);
            animateCountryDock(null);
          }}
        >
          {dockLabel ? (
            <div className="material-finance-dock-label" style={{ left: dockLabel.left, top: dockLabel.top }}>
              {dockLabel.text}
            </div>
          ) : null}
          {countryCodes.map((code) => (
            <button
              key={code}
              type="button"
              data-country-code={code}
              className={`material-finance-country-chip${code === countryCode ? " is-active" : ""}`}
              aria-label={formatCountryCodeTooltip(code)}
              onFocus={(event) => {
                setCountryDockLabel(code, event.currentTarget);
                const rect = event.currentTarget.getBoundingClientRect();
                animateCountryDock(rect.left + rect.width / 2);
              }}
              onBlur={() => {
                setDockLabel(null);
                animateCountryDock(null);
              }}
              onPointerEnter={(event) => setCountryDockLabel(code, event.currentTarget)}
              onClick={() => {
                if (code !== countryCode) void onCountryChange(code);
              }}
            >
              {code}
            </button>
          ))}
        </div>
      </header>
      {loading ? (
        <div className="material-finance-empty">Loading finance rows...</div>
      ) : !isNlPriceCbu ? (
        <CountryCbuPastePanel
          countryCode={countryCode}
          rows={rows}
          savingMaterialCode={savingMaterialCode}
          onPreviewImport={onPreviewImport}
          onSaveRow={onSaveRow}
        />
      ) : (
        <MaterialFinanceMatrix
          rows={rows}
          title="NL CBU / margin matrix"
          savingMaterialCode={savingMaterialCode}
          onViewHistory={onViewHistory}
          onSaveRow={onSaveRow}
        />
      )}
      {error ? <div className="material-finance-error">{error}</div> : null}
    </section>
  );
}
