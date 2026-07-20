import { describe, expect, it } from "vitest";

import { resolveCommonParentMaterialRemark } from "../../utils/orderGeniusRemarks";

type RemarkRow = {
  countryCode: string;
  fatherMaterial: string;
  note?: string;
};

const fatherMaterialForRow = (row: RemarkRow): string => row.fatherMaterial;
const noteForRow = (row: RemarkRow): string | undefined => row.note;

describe("Order Genius father note grouping", () => {
  it("shows the father note on a collapsed group with one father material", () => {
    const rows: RemarkRow[] = [
      { countryCode: "AT", fatherMaterial: "T7000SW**MY0001", note: "法规升级" },
      { countryCode: "AT", fatherMaterial: "T7000SW**MY0001" },
    ];

    expect(resolveCommonParentMaterialRemark(rows, fatherMaterialForRow, noteForRow))
      .toBe("法规升级");
  });

  it("does not merge different father notes into the outer trim group", () => {
    const firstFather: RemarkRow[] = [
      { countryCode: "AT", fatherMaterial: "T7000SW**MY0001", note: "法规升级" },
    ];
    const secondFather: RemarkRow[] = [
      { countryCode: "AT", fatherMaterial: "T7000SW**MY0002", note: "高配版本" },
    ];
    const trimRows = [...firstFather, ...secondFather];

    expect(resolveCommonParentMaterialRemark(trimRows, fatherMaterialForRow, noteForRow))
      .toBeUndefined();
    expect(resolveCommonParentMaterialRemark(firstFather, fatherMaterialForRow, noteForRow))
      .toBe("法规升级");
    expect(resolveCommonParentMaterialRemark(secondFather, fatherMaterialForRow, noteForRow))
      .toBe("高配版本");
  });

  it("shows the same father note when the grouped rows span countries", () => {
    const rows: RemarkRow[] = [
      { countryCode: "AT", fatherMaterial: "T7000SW**MY0001", note: "法规升级" },
      { countryCode: "CZ", fatherMaterial: "T7000SW**MY0001", note: "法规升级" },
      { countryCode: "SE", fatherMaterial: "T7000SW**MY0001", note: "法规升级" },
    ];

    expect(resolveCommonParentMaterialRemark(rows, fatherMaterialForRow, noteForRow))
      .toBe("法规升级");
  });
});
