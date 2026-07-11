import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const Babel = require("@babel/standalone");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const files = ["index.html", "ADMINPRO.html", "wholesale.html", "invoice.html"];

let failed = false;
for (const name of files) {
  const source = await readFile(resolve(root, name), "utf8");
  const scripts = [...source.matchAll(/<script\s+type=["']text\/babel["'][^>]*>([\s\S]*?)<\/script>/gi)];
  if (!scripts.length) {
    process.stderr.write(`${name}: no text/babel script found\n`);
    failed = true;
    continue;
  }
  try {
    for (const match of scripts) {
      Babel.transform(match[1], { presets: ["react"], sourceType: "script" });
    }
    process.stdout.write(`${name}: JSX syntax OK\n`);
  } catch (error) {
    process.stderr.write(`${name}: ${error.message}\n`);
    failed = true;
  }
}

if (failed) process.exit(1);

for (const name of ["settings.html", "scanner.html", "B2Binvoice.html"]) {
  const plainSource = await readFile(resolve(root, name), "utf8");
  const plainScripts = [...plainSource.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)];
  try {
    for (const match of plainScripts) new Function(match[1]);
    process.stdout.write(`${name}: JavaScript syntax OK\n`);
  } catch (error) {
    process.stderr.write(`${name}: ${error.message}\n`);
    process.exit(1);
  }
}
