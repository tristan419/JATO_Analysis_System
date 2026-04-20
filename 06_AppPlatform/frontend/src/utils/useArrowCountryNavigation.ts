import { useEffect, useMemo } from "react";

import {
  getAdjacentValuedItem,
  getVerticalNavigationDirectionFromKey,
  shouldIgnorePageNavigationTarget,
} from "./pageNavigation";

interface ArrowCountryNavigationOption {
  value: string;
  label?: string;
}

interface UseArrowCountryNavigationOptions {
  options: ArrowCountryNavigationOption[];
  activeValue: string;
  onSelect: (value: string) => void;
}

export function useArrowCountryNavigation({
  options,
  activeValue,
  onSelect,
}: UseArrowCountryNavigationOptions) {
  const previousCountry = useMemo(
    () => getAdjacentValuedItem(options, activeValue, -1),
    [activeValue, options],
  );
  const nextCountry = useMemo(
    () => getAdjacentValuedItem(options, activeValue, 1),
    [activeValue, options],
  );

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        event.defaultPrevented
        || event.altKey
        || event.ctrlKey
        || event.metaKey
        || event.shiftKey
        || event.repeat
      ) {
        return;
      }
      const direction = getVerticalNavigationDirectionFromKey(event.key);
      if (direction === null || shouldIgnorePageNavigationTarget(event.target)) {
        return;
      }
      const targetCountry = direction < 0 ? previousCountry : nextCountry;
      if (!targetCountry) {
        return;
      }
      event.preventDefault();
      onSelect(targetCountry.value);
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [nextCountry, onSelect, previousCountry]);
}
