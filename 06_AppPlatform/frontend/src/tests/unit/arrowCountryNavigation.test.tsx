// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";

import { useArrowCountryNavigation } from "../../utils/useArrowCountryNavigation";

const COUNTRY_OPTIONS = [
  { value: "瑞典", label: "瑞典" },
  { value: "挪威", label: "挪威" },
  { value: "芬兰", label: "芬兰" },
];

function TestHarness({ initialCountry = "挪威" }: { initialCountry?: string }) {
  const [country, setCountry] = useState(initialCountry);
  useArrowCountryNavigation({
    options: COUNTRY_OPTIONS,
    activeValue: country,
    onSelect: setCountry,
  });

  return (
    <div>
      <div>{`Active country: ${country}`}</div>
      <input aria-label="demo input" />
    </div>
  );
}

describe("useArrowCountryNavigation", () => {
  afterEach(() => {
    cleanup();
  });

  it("supports up/down country navigation without wrapping", () => {
    render(<TestHarness />);

    fireEvent.keyDown(window, { key: "ArrowUp" });
    expect(screen.getByText("Active country: 瑞典")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowUp" });
    expect(screen.getByText("Active country: 瑞典")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(screen.getByText("Active country: 挪威")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowDown" });
    expect(screen.getByText("Active country: 芬兰")).toBeTruthy();
  });

  it("does not hijack left/right keys", () => {
    render(<TestHarness />);

    fireEvent.keyDown(window, { key: "ArrowLeft" });
    expect(screen.getByText("Active country: 挪威")).toBeTruthy();

    fireEvent.keyDown(window, { key: "ArrowRight" });
    expect(screen.getByText("Active country: 挪威")).toBeTruthy();
  });

  it("does not hijack arrow keys while an input is focused", () => {
    render(<TestHarness />);

    const input = screen.getByLabelText("demo input");
    input.focus();
    fireEvent.keyDown(input, { key: "ArrowDown" });

    expect(screen.getByText("Active country: 挪威")).toBeTruthy();
  });
});
