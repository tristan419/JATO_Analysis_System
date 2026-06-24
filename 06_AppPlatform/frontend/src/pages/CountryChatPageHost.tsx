import { CountryChatProvider } from "../contexts/CountryChatContext";
import { CountryChatPage } from "./CountryChatPage";

export function CountryChatPageHost() {
  return (
    <CountryChatProvider>
      <CountryChatPage />
    </CountryChatProvider>
  );
}
