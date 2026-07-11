import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const copies = [
  ["node_modules/react/umd/react.production.min.js", "assets/vendor/react.production.min.js"],
  ["node_modules/react-dom/umd/react-dom.production.min.js", "assets/vendor/react-dom.production.min.js"],
  ["node_modules/@babel/standalone/babel.min.js", "assets/vendor/babel.min.js"],
  ...[300, 400, 500, 600, 700, 800, 900].map((weight) => [
    `node_modules/@fontsource/inter/files/inter-latin-${weight}-normal.woff2`,
    `assets/fonts/inter-latin-${weight}-normal.woff2`
  ]),
  ...[400, 500, 700, 800].map((weight) => [
    `node_modules/@fontsource/jetbrains-mono/files/jetbrains-mono-latin-${weight}-normal.woff2`,
    `assets/fonts/jetbrains-mono-latin-${weight}-normal.woff2`
  ])
];

for (const [source, destination] of copies) {
  const from = resolve(root, source);
  const to = resolve(root, destination);
  await mkdir(dirname(to), { recursive: true });
  await copyFile(from, to);
}

process.stdout.write(`Vendored ${copies.length} browser assets.\n`);
