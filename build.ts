//参考:https://zenn.dev/rabbit/articles/ef84ae02a987b2

import { Command } from "commander";
import { build, BuildOptions } from "esbuild";
import fs from "fs-extra";
const { dependencies } = fs.readJsonSync("./package.json");
const program = new Command();
program.option("-m --minify", "minify");
program.parse(process.argv);
const options = program.opts();
const minify = options.minify;

const entryFile = "src/index.ts";

const shared: BuildOptions = {
  entryPoints: [entryFile],
  bundle: true,
  external: Object.keys(dependencies),
  charset: "utf8",
  minify: minify || false,
  treeShaking: true,
  sourcemap: true,
  plugins: [],
};
// console.log({ options, shared });
const runner = async () => {
  const esmResult = await build({
    ...shared,
    format: "esm",
    outfile: "./dist/index.esm.js",
    target: ["ES6"],
  });
  const cjsResult = await build({
    ...shared,
    format: "cjs",
    outfile: "./dist/index.cjs.js",
    target: ["ES6"],
  });
  const binResult = await build({
    ...shared,
    platform: "node",
    outfile: "./dist/metabase-bigquery-transfer.js",
    target: ["ES6"],
    entryPoints: ["src/metabase-bigquery-transfer.ts"],
  });
  return { esmResult, cjsResult };
};

runner().then((result) => {
  // console.log({ result });
  process.exit(0);
});
