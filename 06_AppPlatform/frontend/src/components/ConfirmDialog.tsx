import type { ReactNode } from "react";
import { useEffect, useId, useRef } from "react";
import { createPortal } from "react-dom";

import { LoadingActionButton } from "./LoadingActionButton";

export type ConfirmDialogTone = "default" | "warning" | "danger";

export interface ConfirmDialogErrorDetail {
  label: string;
  value: string;
}

export interface ConfirmDialogError {
  title: string;
  message: string;
  details?: ConfirmDialogErrorDetail[];
  sourceFeedback?: string;
  retryBlocked?: boolean;
}

interface ConfirmDialogProps {
  title: string;
  description: ReactNode;
  children?: ReactNode;
  cancelLabel: string;
  confirmLabel: string;
  loadingLabel: string;
  submitting: boolean;
  error: ConfirmDialogError | null;
  tone?: ConfirmDialogTone;
  onCancel: () => void;
  onConfirm: () => void;
}

const focusableSelector = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

export function ConfirmDialog({
  title,
  description,
  children,
  cancelLabel,
  confirmLabel,
  loadingLabel,
  submitting,
  error,
  tone = "default",
  onCancel,
  onConfirm,
}: ConfirmDialogProps) {
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const cancelButtonRef = useRef<HTMLButtonElement | null>(null);
  const onCancelRef = useRef(onCancel);
  const submittingRef = useRef(submitting);

  useEffect(() => {
    onCancelRef.current = onCancel;
    submittingRef.current = submitting;
  }, [onCancel, submitting]);

  useEffect(() => {
    const previouslyFocused = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const focusFrame = window.requestAnimationFrame(() => {
      cancelButtonRef.current?.focus();
    });

    function handleKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") {
        if (!submittingRef.current) {
          event.preventDefault();
          onCancelRef.current();
        }
        return;
      }
      if (event.key !== "Tab" || !dialogRef.current) {
        return;
      }
      const focusableElements = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(focusableSelector)
      ).filter((element) => (
        !element.hasAttribute("disabled")
        && element.getAttribute("aria-hidden") !== "true"
      ));
      if (focusableElements.length === 0) {
        event.preventDefault();
        dialogRef.current.focus();
        return;
      }
      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];
      const activeElement = document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
      if (
        !activeElement
        || !dialogRef.current.contains(activeElement)
        || !focusableElements.includes(activeElement)
      ) {
        event.preventDefault();
        (event.shiftKey ? lastElement : firstElement).focus();
      } else if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, []);

  return createPortal(
    <div
      className="confirm-dialog-overlay"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget && !submitting) {
          onCancel();
        }
      }}
    >
      <div
        ref={dialogRef}
        className="confirm-dialog"
        data-tone={tone}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        aria-busy={submitting || undefined}
        tabIndex={-1}
      >
        <header className="confirm-dialog-header">
          <span className="confirm-dialog-eyebrow">JATO DATA OPS / ACTION CONFIRMATION</span>
          <h2 id={titleId}>{title}</h2>
          <div id={descriptionId} className="confirm-dialog-description">
            {description}
          </div>
        </header>

        <div className="confirm-dialog-body">
          {children}
          {error ? (
            <section className="confirm-dialog-error" role="alert" aria-live="assertive">
              <strong>{error.title}</strong>
              <p>{error.message}</p>
              {error.details?.length ? (
                <dl>
                  {error.details.map((detail) => (
                    <div key={`${detail.label}-${detail.value}`}>
                      <dt>{detail.label}</dt>
                      <dd>{detail.value}</dd>
                    </div>
                  ))}
                </dl>
              ) : null}
              {error.sourceFeedback ? (
                <div className="confirm-dialog-source-feedback">
                  <span>给洗数人员</span>
                  <p>{error.sourceFeedback}</p>
                </div>
              ) : null}
              {error.retryBlocked ? (
                <p className="confirm-dialog-retry-blocked">
                  为防止未知状态下重复写入，本弹窗已锁定再次提交；请返回并刷新任务状态。
                </p>
              ) : null}
            </section>
          ) : null}
        </div>

        <footer className="confirm-dialog-footer">
          <button
            ref={cancelButtonRef}
            type="button"
            className="btn btn-secondary"
            disabled={submitting}
            onClick={onCancel}
          >
            {cancelLabel}
          </button>
          <LoadingActionButton
            variant={tone === "danger" ? "danger" : "primary"}
            loading={submitting}
            loadingLabel={loadingLabel}
            disabled={error?.retryBlocked}
            onClick={onConfirm}
          >
            {confirmLabel}
          </LoadingActionButton>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
