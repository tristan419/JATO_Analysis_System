// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SearchSelectFilter } from "../../components/SearchSelectFilter";
import { matchesCompactSearch, optionMatchesCompactSearch } from "../../utils/searchMatching";

describe("SearchSelectFilter", () => {
  afterEach(() => {
    cleanup();
  });

  it("matches options when users omit spaces and punctuation", () => {
    expect(matchesCompactSearch("ATTO 2", "atto2")).toBe(true);
    expect(matchesCompactSearch("LYNK & CO 02", "lynkco02")).toBe(true);
    expect(matchesCompactSearch("ŠKODA", "skoda")).toBe(true);
  });

  it("matches option labels and values with the same compact rules", () => {
    expect(optionMatchesCompactSearch({ label: "SUV A0", value: "SUV A0" }, "suva0")).toBe(true);
    expect(optionMatchesCompactSearch({ label: "Czech Republic", value: "捷克共和国" }, "czechrepublic")).toBe(true);
  });

  it("uses compact matching in the visible option list", () => {
    render(
      <SearchSelectFilter
        label="车型"
        options={["ATTO 2", "LYNK & CO 02", "ŠKODA Enyaq"]}
        selected={[]}
        onChange={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("搜索 车型…"), { target: { value: "lynkco02" } });

    expect(screen.getByText("LYNK & CO 02")).toBeTruthy();
    expect(screen.queryByText("ATTO 2")).toBeNull();
  });
});
