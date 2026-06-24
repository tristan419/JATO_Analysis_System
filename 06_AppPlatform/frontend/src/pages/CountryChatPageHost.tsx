import { CountryChatProvider } from "../contexts/CountryChatContext";
import { CountryChatPage } from "./CountryChatPage";
import "../countryCopilot.css";

export function CountryChatPageHost() {
  return (
    <CountryChatProvider>
      <CountryChatPage />
    </CountryChatProvider>
  );
}
