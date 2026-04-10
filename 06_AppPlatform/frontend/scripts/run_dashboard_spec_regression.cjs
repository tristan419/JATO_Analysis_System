const { spawn } = require('node:child_process');
const net = require('node:net');
const path = require('node:path');

const frontendRoot = path.resolve(__dirname, '..');
const host = process.env.JATO_REGRESSION_HOST || '127.0.0.1';
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveBaseUrl(port) {
  return process.env.JATO_REGRESSION_BASE_URL || `http://${host}:${port}`;
}

async function resolvePort() {
  if (process.env.JATO_REGRESSION_PORT) return process.env.JATO_REGRESSION_PORT;

  return new Promise((resolve, reject) => {
    const probeServer = net.createServer();
    probeServer.unref();
    probeServer.on('error', reject);
    probeServer.listen(0, host, () => {
      const address = probeServer.address();
      if (!address || typeof address === 'string') {
        probeServer.close(() => reject(new Error('Unable to resolve an available preview port')));
        return;
      }

      probeServer.close((closeError) => {
        if (closeError) {
          reject(closeError);
          return;
        }
        resolve(String(address.port));
      });
    });
  });
}

async function waitForServer(previewProcess, baseUrl) {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    if (previewProcess.exitCode !== null) {
      throw new Error(`Preview server exited early with code ${previewProcess.exitCode}`);
    }

    try {
      const response = await fetch(baseUrl);
      if (response.ok) return;
    } catch {
      // Server is still starting.
    }

    await wait(500);
  }

  throw new Error(`Timed out waiting for preview server at ${baseUrl}`);
}

function runRegression(baseUrl) {
  return new Promise((resolve, reject) => {
    const regressionProcess = spawn(
      process.execPath,
      [path.join(frontendRoot, 'scripts', 'dashboard_spec_mock_regression.cjs')],
      {
        cwd: frontendRoot,
        env: {
          ...process.env,
          JATO_REGRESSION_BASE_URL: baseUrl,
        },
        stdio: 'inherit',
      },
    );

    regressionProcess.on('error', reject);
    regressionProcess.on('exit', (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(signal
        ? `Regression process terminated with signal ${signal}`
        : `Regression process exited with code ${code}`));
    });
  });
}

async function stopProcess(childProcess) {
  if (!childProcess || childProcess.exitCode !== null) return;

  childProcess.kill('SIGTERM');
  for (let attempt = 0; attempt < 10; attempt += 1) {
    if (childProcess.exitCode !== null) return;
    await wait(250);
  }

  childProcess.kill('SIGKILL');
}

async function main() {
  const port = await resolvePort();
  const baseUrl = resolveBaseUrl(port);
  const previewProcess = spawn(
    npmCommand,
    ['run', 'preview', '--', '--host', host, '--port', port, '--strictPort'],
    {
      cwd: frontendRoot,
      env: process.env,
      stdio: 'inherit',
    },
  );

  const shutdown = async () => {
    await stopProcess(previewProcess);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  try {
    await waitForServer(previewProcess, baseUrl);
    await runRegression(baseUrl);
  } finally {
    process.off('SIGINT', shutdown);
    process.off('SIGTERM', shutdown);
    await stopProcess(previewProcess);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});