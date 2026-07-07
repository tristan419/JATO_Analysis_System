// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";

import { CommandMultiSelect, type CommandSelectOption } from "../../components/CommandSelect";

const COUNTRY_OPTIONS: Array<CommandSelectOption<string>> = [
  { value: "AT", label: "AT", caption: "Austria · 奥地利" },
  { value: "BE", label: "BE", caption: "Belgium · 比利时" },
  { value: "CZ", label: "CZ", caption: "Czechia · 捷克" },
];

function MultiSelectHarness({ maxSelected }: { maxSelected?: number }) {
  const [selected, setSelected] = useState<string[]>(["AT"]);

  return (
    <div>
      <CommandMultiSelect
        label="Countries"
        selected={selected}
        options={COUNTRY_OPTIONS}
        onChange={setSelected}
        searchPlaceholder="Search country..."
        maxSelected={maxSelected}
      />
      <div>{`Selected: ${selected.join("/")}`}</div>
    </div>
  );
}

describe("CommandMultiSelect", () => {
  afterEach(() => {
    cleanup();
  });

  it("supports keyboard selection with arrow keys and enter", () => {
    render(<MultiSelectHarness />);

    fireEvent.click(screen.getByRole("button", { name: "Countries" }));
    const search = screen.getByPlaceholderText("Search country...");

    fireEvent.keyDown(search, { key: "ArrowDown" });
    fireEvent.keyDown(search, { key: "Enter" });

    expect(screen.getByText("Selected: AT/BE")).toBeTruthy();
  });

  it("closes the popover with escape", () => {
    render(<MultiSelectHarness />);

    fireEvent.click(screen.getByRole("button", { name: "Countries" }));
    expect(screen.getByPlaceholderText("Search country...")).toBeTruthy();

    fireEvent.keyDown(screen.getByPlaceholderText("Search country..."), { key: "Escape" });

    expect(screen.queryByPlaceholderText("Search country...")).toBeNull();
  });

  it("respects max selected during keyboard selection", () => {
    render(<MultiSelectHarness maxSelected={1} />);

    fireEvent.click(screen.getByRole("button", { name: "Countries" }));
    const search = screen.getByPlaceholderText("Search country...");

    fireEvent.keyDown(search, { key: "ArrowDown" });
    fireEvent.keyDown(search, { key: "Enter" });

    expect(screen.getByText("Selected: AT")).toBeTruthy();
  });
});
