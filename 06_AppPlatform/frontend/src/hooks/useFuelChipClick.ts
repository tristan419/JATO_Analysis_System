import { useCallback } from "react";

export function useFuelChipClick(
  fuelOptions: string[],
  setSelectedFuelTypes: (value: string[] | ((prev: string[]) => string[])) => void,
) {
  const toggle = useCallback(
    (fuel: string) => {
      setSelectedFuelTypes((current) => {
        if (current.includes(fuel)) {
          return current.length > 1 ? current.filter((item) => item !== fuel) : current;
        }
        return [...current, fuel];
      });
    },
    [setSelectedFuelTypes],
  );

  const isolate = useCallback(
    (fuel: string) => {
      setSelectedFuelTypes((current) => {
        if (current.length === 1 && current[0] === fuel) return fuelOptions;
        return [fuel];
      });
    },
    [fuelOptions, setSelectedFuelTypes],
  );

  return { toggle, isolate } as const;
}
