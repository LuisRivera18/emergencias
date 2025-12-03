// src/node.ts
import fs from "fs";
import path from "path";
import readline from "readline";
import { DB } from "./db";
import { ClusterNode } from "./clusterNode";

const CONFIG_PATH = process.argv[2] || "./config.json";

if (!fs.existsSync(CONFIG_PATH)) {
  console.error("Falta archivo de configuración. Ejecuta: node dist/node.js ./config-nodeX.json");
  process.exit(1);
}

const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
const dbFile = config.dbFile || "./db.sqlite";
const schemaFile = config.schemaFile || path.resolve(__dirname, "..", "schema.sql");

const db = new DB(dbFile, schemaFile);
const node = new ClusterNode(
  {
    nodeId: config.nodeId,
    host: config.host,
    tcpPort: config.tcpPort,
    peers: config.peers,
    initialLeader: config.initialLeader
  },
  db
);

console.log(Nodo ${config.nodeId} iniciado. Comandos:);
console.log(`  visit <pacienteId> <trabajadorSocialId> <sala>`);
console.log(`  close <visitId>`);
console.log(`  list`);
console.log(`  exit`);

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: true
});

rl.on("line", (line) => {
  const parts = line.trim().split(/\s+/);
  const cmd = parts[0];

  if (cmd === "visit") {
    const pacienteId = parseInt(parts[1], 10);
    const trabajadorId = parseInt(parts[2], 10);
    const sala = parts[3];
    if (!pacienteId || !trabajadorId || !sala) {
      console.log("Uso: visit <pacienteId> <trabajadorSocialId> <sala>");
      return;
    }
    node.clientCreateVisit(pacienteId, trabajadorId, sala);
  } else if (cmd === "close") {
    const visitId = parseInt(parts[1], 10);
    if (!visitId) {
      console.log("Uso: close <visitId>");
      return;
    }
    node.clientCloseVisit(visitId);
  } else if (cmd === "list") {
    node.listVisits();
  } else if (cmd === "exit") {
    process.exit(0);
  } else if (cmd.length > 0) {
    console.log("Comandos válidos: visit, close, list, exit");
  }
});
