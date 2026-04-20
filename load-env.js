"use strict";

const path = require("path");

// Load Fluidra/.env when present (see .env.example; never commit .env).
require("dotenv").config({ path: path.join(__dirname, ".env") });

function requireEnv(name) {
  const v = process.env[name];
  if (v == null || String(v).trim() === "") {
    console.error(
      `[Fluidra] Missing required environment variable: ${name}\n` +
        `Copy .env.example to .env in this folder and set your credentials.`
    );
    process.exit(1);
  }
  return String(v).trim();
}

module.exports = { requireEnv };
