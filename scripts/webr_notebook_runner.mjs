#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";

import { WebR } from "webr";

function usage() {
  console.error("Usage: node scripts/webr_notebook_runner.mjs --input spec.json --output result.json");
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (key === "--input" || key === "--output") {
      if (!value) {
        usage();
        process.exit(2);
      }
      args[key.slice(2)] = value;
      i += 1;
      continue;
    }
    usage();
    process.exit(2);
  }
  if (!args.input || !args.output) {
    usage();
    process.exit(2);
  }
  return args;
}

function streamText(result) {
  if (!result) return "";
  if (Array.isArray(result.output)) {
    return result.output
      .filter((item) => item && (item.type === "stdout" || item.type === "stderr"))
      .map((item) => String(item.data || ""))
      .join("\n")
      .replace(/\n{3,}/g, "\n\n");
  }
  return String(result.stdout || "") + String(result.stderr || "");
}

async function fileExists(webR, path) {
  try {
    await webR.FS.lookupPath(path);
    return true;
  } catch (_) {
    return false;
  }
}

async function readFileAsDataURI(webR, path, mime) {
  const bytes = await webR.FS.readFile(path);
  const encoded = Buffer.from(bytes).toString("base64");
  return `data:${mime};base64,${encoded}`;
}

async function captureCell(webR, cell) {
  const code = String(cell.source || "");
  const plotPath = String(cell.plot_path || "");
  if (plotPath) {
    try {
      await webR.evalRVoid(`unlink(${JSON.stringify(plotPath)})`);
    } catch (_) {
      // The plot path may not exist in a fresh webR filesystem.
    }
  }

  const shelter = await new webR.Shelter();
  const result = await shelter.captureR(code, {
    withAutoprint: true,
    captureStreams: true,
  });
  try {
    await shelter.purge();
  } catch (_) {
    // Older webR builds may clean shelters automatically.
  }

  if (plotPath && (await fileExists(webR, plotPath))) {
    return {
      id: cell.id,
      output: {
        type: "image",
        src: await readFileAsDataURI(webR, plotPath, "image/svg+xml"),
        inlineSize: "small",
      },
    };
  }

  const text = streamText(result).replace(/\u001b\[[0-9;]*[A-Za-z]/g, "").trimEnd();
  return {
    id: cell.id,
    output: {
      type: "text",
      text: text ? `${text}\n` : "Done (no output)",
    },
  };
}

function cellFailureContext(cell) {
  const code = String(cell?.source || "");
  const firstLine = code.split(/\r?\n/).find((line) => line.trim()) || "R cell";
  const style = cell?.validation_style ? ` style=${cell.validation_style}` : "";
  return `webR cell ${cell?.id ?? "unknown"}${style} failed: ${firstLine.slice(0, 160)}`;
}

async function installRequiredPackages(webR, packages) {
  const uniquePackages = [...new Set((packages || []).map((value) => String(value || "").trim()).filter(Boolean))];
  const installed = [];
  for (const packageName of uniquePackages) {
    try {
      await webR.installPackages([packageName], { quiet: true });
      const available = await webR.evalRString(
        `if (requireNamespace(${JSON.stringify(packageName)}, quietly = TRUE)) "true" else "false"`,
      );
      if (String(available) !== "true") {
        throw new Error("package did not load after installation");
      }
      const version = await webR.evalRString(
        `as.character(utils::packageVersion(${JSON.stringify(packageName)}))`,
      );
      installed.push({ package: packageName, version: String(version || "") });
    } catch (error) {
      throw new Error(`required WebAssembly R package failed: ${packageName}`);
    }
  }
  return installed;
}

async function main() {
  const args = parseArgs(process.argv);
  const spec = JSON.parse(await readFile(args.input, "utf8"));
  const webR = new WebR();
  await webR.init();
  const installedPackages = await installRequiredPackages(webR, spec.required_packages);

  const rResults = [];
  for (const cell of spec.cells || []) {
    if (cell && cell.mode === "r") {
      try {
        rResults.push(await captureCell(webR, cell));
      } catch (error) {
        const detail = error && error.stack ? error.stack : String(error);
        throw new Error(`${cellFailureContext(cell)}\n${detail}`);
      }
    }
  }

  const sessionInfo = await webR.evalRString('paste(capture.output(sessionInfo()), collapse="\\n")');
  try {
    await webR.close();
  } catch (_) {
    // No-op on runtimes that do not expose close().
  }

  await writeFile(
    args.output,
    JSON.stringify(
      {
        schema: "web-r.notebook.runner-result.v1",
        runtime: "webr",
        packages: installedPackages,
        r_results: rResults,
        runtime_sessionInfo: String(sessionInfo || ""),
      },
      null,
      2,
    ),
    "utf8",
  );
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
