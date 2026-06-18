import { describe, expect, it } from "vitest";
import { getOAuthRedirectTarget } from "../../utils/oauthRedirect";

describe("getOAuthRedirectTarget", () => {
  it("keeps the current app page after an existing-user OAuth callback", () => {
    expect(
      getOAuthRedirectTarget(
        {
          pathname: "/product/order-genius",
          search: "?token=abc&username=test&role=order_filler&country=DK",
          hash: "#bom",
        },
        false,
      ),
    ).toBe("/product/order-genius?country=DK#bom");
  });

  it("sends new users to profile setup", () => {
    expect(
      getOAuthRedirectTarget(
        {
          pathname: "/product/order-genius",
          search: "?token=abc&username=test&role=viewer&isNewUser=true",
          hash: "",
        },
        true,
      ),
    ).toBe("/account/profile");
  });

  it("falls back to dashboard only for an empty pathname", () => {
    expect(
      getOAuthRedirectTarget(
        {
          pathname: "",
          search: "?token=abc",
          hash: "",
        },
        false,
      ),
    ).toBe("/dashboard");
  });
});
