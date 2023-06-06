import type { Config } from "jest";

const config: Config = {
  testMatch: [
    "**/__tests__/**/*.+(ts|tsx|js)",
    "**/?(*.)+(spec|test).+(ts|tsx|js)",
  ],
  transform: {
    "^.+\\.(ts|tsx)$": "ts-jest",
  },
  watchPathIgnorePatterns: ["<rootDir>/__tests__/tmp/"],
};

export default config;
