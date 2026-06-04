# CODEX.md

## Local Development

- Frontend dev server defaults to the existing Vite instance at `http://127.0.0.1:5173/`.
- Before opening or verifying the frontend, check whether `5173` is already responding:
  `curl -I --max-time 3 http://127.0.0.1:5173/`
- If `5173` responds normally, reuse it. Do not start another Vite server on `5174`.
- Only start the frontend server when `5173` is not responding, from `06_AppPlatform/frontend`:
  `npm run dev -- --host 127.0.0.1 --port 5173`
- If Vite reports that `5173` is occupied, do not silently switch to `5174`; first verify whether the existing `5173` instance is usable and mention it in the reply.

