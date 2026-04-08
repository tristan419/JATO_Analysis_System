import { useState } from "react";

export function SearchSelectFilter({
  label,
  options,
  selected,
  onChange,
  showSuvShortcut = false,
}: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (vals: string[]) => void;
  showSuvShortcut?: boolean;
}) {
  const [query, setQuery] = useState("");
  const normalizedQuery = query.toLowerCase().trim();
  const matched = normalizedQuery
    ? options.filter((option) => option.toLowerCase().includes(normalizedQuery))
    : options;
  const selectedSet = new Set(selected);

  return (
    <div className="filter-card">
      <div className="filter-card-head">
        <div className="filter-card-title">{label}</div>
        <div className="filter-card-count">{String(selected.length).padStart(2, "0")}</div>
      </div>
      <input
        type="text"
        className="filter-search"
        placeholder={`搜索 ${label}…`}
        value={query}
        onChange={(event) => setQuery(event.target.value)}
      />
      <div className="filter-actions">
        <button
          type="button"
          className="filter-action-btn"
          onClick={() => {
            const next = new Set(selected);
            matched.forEach((item) => next.add(item));
            onChange(Array.from(next));
          }}
        >
          全选搜索结果
        </button>
        {showSuvShortcut && options.some((option) => option.toLowerCase().includes("suv")) && (
          <button
            type="button"
            className="filter-action-btn"
            onClick={() => onChange(options.filter((option) => option.toLowerCase().includes("suv")))}
          >
            一键筛选 SUV
          </button>
        )}
        <button type="button" className="filter-action-btn" onClick={() => onChange([])}>
          清空
        </button>
      </div>
      <div className="filter-options-list">
        {matched.slice(0, 200).map((option) => (
          <label key={option} className="filter-option">
            <input
              type="checkbox"
              checked={selectedSet.has(option)}
              onChange={() =>
                onChange(
                  selectedSet.has(option)
                    ? selected.filter((item) => item !== option)
                    : [...selected, option]
                )
              }
            />
            <span>{option}</span>
          </label>
        ))}
        {matched.length === 0 && <div className="filter-empty">无匹配项</div>}
      </div>
      <div className="filter-summary">匹配 {matched.length} 项｜已选 {selected.length} 项</div>
    </div>
  );
}