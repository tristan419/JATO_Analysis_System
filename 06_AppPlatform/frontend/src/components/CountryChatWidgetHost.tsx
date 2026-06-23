import { CountryChatProvider } from "../contexts/CountryChatContext";
import { CountryChatWidget } from "./CountryChatWidget";

export function CountryChatWidgetHost() {
  return (
    <CountryChatProvider>
      <CountryChatWidget />
    </CountryChatProvider>
  );
}
