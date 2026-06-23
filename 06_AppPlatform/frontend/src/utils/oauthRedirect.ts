export interface OAuthRedirectLocation {
  pathname: string;
  search: string;
  hash: string;
}

export function getOAuthRedirectTarget(
  location: OAuthRedirectLocation,
  isNewUser: boolean,
): string {
  if (isNewUser) return "/account/profile";

  const cleanParams = new URLSearchParams(location.search);
  cleanParams.delete("token");
  cleanParams.delete("username");
  cleanParams.delete("role");
  cleanParams.delete("isNewUser");

  const cleanSearch = cleanParams.toString();
  const pathname = location.pathname || "/dashboard";
  return `${pathname}${cleanSearch ? `?${cleanSearch}` : ""}${location.hash}`;
}
