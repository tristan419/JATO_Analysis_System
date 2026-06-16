import { useEffect, useMemo, useState } from "react";

import { api } from "../api/client";
import type { CountryPaymentTerm } from "../types/orderGenius";
import {
  JATO_COUNTRIES,
  type JatoCountryOption,
} from "../utils/jatoCountries";

function toCountryOption(country: CountryPaymentTerm): JatoCountryOption {
  const countryCode = country.countryCode.trim().toUpperCase();
  return {
    countryCode,
    countryName: country.countryName || countryCode,
    countryNameZh: country.countryName || countryCode,
    marketScanCountry: country.countryName || countryCode,
  };
}

export function useAccountCountryOptions(): {
  countryOptions: JatoCountryOption[];
  loading: boolean;
  error: string;
} {
  const [orderingCountries, setOrderingCountries] = useState<CountryPaymentTerm[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    api.getAccountCountryOptions()
      .then((response) => {
        if (!active) return;
        setOrderingCountries(response.items || []);
      })
      .catch((err: unknown) => {
        if (!active) return;
        setOrderingCountries([]);
        setError(err instanceof Error ? err.message : "Failed to load country options");
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const countryOptions = useMemo(() => {
    const byCode = new Map<string, JatoCountryOption>();
    for (const country of JATO_COUNTRIES) {
      byCode.set(country.countryCode, country);
    }
    for (const country of orderingCountries) {
      const option = toCountryOption(country);
      byCode.set(option.countryCode, byCode.get(option.countryCode) ?? option);
    }
    return Array.from(byCode.values());
  }, [orderingCountries]);

  return { countryOptions, loading, error };
}
