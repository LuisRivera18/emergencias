import express from "express";
import bodyParser from "body-parser";
import fs from "fs";
import path from "path";
import axios from "axios";
import { DB } from "./db";
import { Replicator } from "./replication";
import { Bully } from "./election";
import winston from "winston";

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

const CONFIG_PATH = process.argv[2] || "./config.json";
if (!fs.existsSync(CONFIG_PATH)) {
  console.error("Falta config.json. Ejecuta con: node dist/server.js ./config.json");
  process.exit(1);
}
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));

const app = express();
app.use(bodyParser.json());

const dbFile = config.dbFile || "./db.sqlite";
const schema = path.resolve(__dirname, "..", "schema.sql");
const db = new DB(dbFile, schema);

// peers: list of base URLs including self (for convenience)
const peers = config.peers as string[]; // e.g., ["http://10.0.0.11:3000", ...]
const selfUrl = config.selfUrl as string;
const nodeId = config.nodeId as number;
const peersInfo = (config.peersInfo as { id: number; url: string }[]) || [];

const replicator = new Replicator(db, peers.filter(p => p !== selfUrl), selfUrl);
const bully = new Bully(nodeId, peersInfo);
let isLeader = false;
let leaderUrl: string | null = null;

// helper: forward to leader if not leader
async function forwardToLeader(req: any, res: any) {
  if (isLeader) return false;
  if (!leaderUrl) {
    return res.status(500).json({ error: "No leader known" });
  }
  // forward path + body
  try {
    const url = leaderUrl + req.path;
    const r = await axios({
      method: req.method,
      url,
      data: req.body,
      headers: req.headers
    });
    res.status(r.status).json(r.data);
  } catch (e: any) {
    res.status(500).json({ error: "Forward to leader failed", details: e.message });
  }
  return true;
}

// Public API (console clients use these)
app.post("/visitas", async (req, res) => {
  // create visit (by trabajador social)
  if (!isLeader) {
    const forwarded = await forwardToLeader(req, res);
    if (forwarded) return;
  }
  const { paciente_id, trabajador_social_id, sala } = req.body;
  if (!paciente_id || !trabajador_social_id || !sala) {
    return res.status(400).json({ error: "falta paciente_id/trabajador_social_id/sala" });
  }

  // create local visit row
  const info = db.run(
    `INSERT INTO VISITAS (paciente_id, sala, trabajador_social_id, estado, consecutivo) VALUES (?, ?, ?, 'ABIERTA', 0)`,
    [paciente_id, sala, trabajador_social_id]
  );
  const visitId = info.lastInsertRowid as number;

  // try to assign doctor+cama (simple strategy: first available)
  const doctor = db.get(`SELECT id FROM DOCTORES WHERE disponible = 1 LIMIT 1`);
  const cama = db.get(`SELECT id FROM CAMAS WHERE disponible = 1 LIMIT 1`);
  if (!doctor || !cama) {
    // replicate create_visit only, assignment later
    const op = { type: "create_visit", data: { paciente_id, sala, trabajador_social_id, visit_id: visitId } };
    const ok = await replicator.replicateOp(op);
    if (ok) {
      return res.json({ success: true, visitId, assigned: false, message: "Creada visita, sin asignar (no hay recursos) " });
    } else {
      return res.status(500).json({ error: "No se pudo replicar create_visit" });
    }
  }

  // assign
  // compute consecutivo: count visits by sala
  const cnt = db.get("SELECT COUNT(*) as c FROM VISITAS WHERE sala = ?", [sala]).c as number;
  const folio = `${paciente_id}-${doctor.id}-${sala}-${cnt}`;

  // do local transaction
  try {
    db.transaction(() => {
      db.run(`UPDATE VISITAS SET doctor_id = ?, cama_id = ?, folio = ?, estado = 'ASIGNADA', consecutivo = ? WHERE id = ?`, [
        doctor.id,
        cama.id,
        folio,
        cnt,
        visitId
      ]);
      db.run(`UPDATE DOCTORES SET disponible = 0 WHERE id = ?`, [doctor.id]);
      db.run(`UPDATE CAMAS SET disponible = 0 WHERE id = ?`, [cama.id]);
    });

    const op = {
      type: "assign_visit",
      data: { visit_id: visitId, doctor_id: doctor.id, cama_id: cama.id, folio, estado: "ASIGNADA", consecutivo: cnt }
    };
    const ok = await replicator.replicateOp(op);
    if (ok) {
      return res.json({ success: true, visitId, doctor: doctor.id, cama: cama.id, folio });
    } else {
      // rollback local (release resources)
      db.transaction(() => {
        db.run(`UPDATE VISITAS SET doctor_id = NULL, cama_id = NULL, folio = NULL, estado = 'ABIERTA' WHERE id = ?`, [visitId]);
        db.run(`UPDATE DOCTORES SET disponible = 1 WHERE id = ?`, [doctor.id]);
        db.run(`UPDATE CAMAS SET disponible = 1 WHERE id = ?`, [cama.id]);
      });
      return res.status(500).json({ error: "No se alcanzó consenso de replicación" });
    }

  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
});

app.post("/visitas/:id/cerrar", async (req, res) => {
  if (!isLeader) {
    const forwarded = await forwardToLeader(req, res);
    if (forwarded) return;
  }
  const id = parseInt(req.params.id, 10);
  const v = db.get(`SELECT * FROM VISITAS WHERE id = ?`, [id]);
  if (!v) return res.status(404).json({ error: "visita no encontrada" });
  if (v.estado === "CERRADA") return res.json({ ok: true, message: "ya cerrada" });

  // close + free resources in local transaction
  try {
    db.transaction(() => {
      db.run(`UPDATE VISITAS SET estado='CERRADA', cerrado_en=CURRENT_TIMESTAMP WHERE id = ?`, [id]);
      if (v.doctor_id) db.run(`UPDATE DOCTORES SET disponible = 1 WHERE id = ?`, [v.doctor_id]);
      if (v.cama_id) db.run(`UPDATE CAMAS SET disponible = 1 WHERE id = ?`, [v.cama_id]);
    });

    const op = { type: "close_visit", data: { visit_id: id, doctor_id: v.doctor_id, cama_id: v.cama_id } };
    const ok = await replicator.replicateOp(op);
    if (ok) return res.json({ ok: true });
    else return res.status(500).json({ error: "No se pudo replicar cierre" });
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
});

// simple list endpoints
app.get("/visitas", (req, res) => {
  const rows = db.all("SELECT * FROM VISITAS ORDER BY creado_en DESC");
  res.json(rows);
});
app.get("/doctores", (req, res) => {
  res.json(db.all("SELECT * FROM DOCTORES"));
});
app.get("/camas", (req, res) => {
  res.json(db.all("SELECT * FROM CAMAS"));
});
app.get("/pacientes", (req, res) => {
  res.json(db.all("SELECT * FROM PACIENTES"));
});

// internal replication endpoint
app.post("/internal/replicate", (req, res) => {
  const op = req.body.op;
  const r = replicator.applyOpLocally(op);
  if (r.ack) res.json({ ack: true });
  else res.status(500).json({ ack: false, error: r.error });
});

// internal election endpoints
app.post("/internal/election", (req, res) => {
  // another node asked "are you alive" as part of election
  res.json({ ok: true, id: nodeId });
});

app.post("/internal/leader", (req, res) => {
  const { leaderId, leaderUrl } = req.body;
  bully.setLeader(leaderId, leaderUrl);
  leaderUrl = leaderUrl;
  isLeader = leaderId === nodeId;
  res.json({ ok: true });
});

// heartbeat endpoint
app.get("/internal/heartbeat", (req, res) => res.json({ ok: true, id: nodeId, url: selfUrl }));

// start server
const PORT = config.port || 3000;
app.listen(PORT, () => {
  logger.info(`Node ${nodeId} listening on ${PORT}. SelfUrl=${selfUrl}`);
  // if config is leader initially
  if (config.initialLeader && config.initialLeader === nodeId) {
    isLeader = true;
    bully.setLeader(nodeId, selfUrl);
    leaderUrl = selfUrl;
  } else {
    leaderUrl = config.initialLeaderUrl || null;
  }
  // start background tasks: heartbeat and leader discovery
  setInterval(async () => {
    // if leader known, check heartbeat
    if (leaderUrl && leaderUrl !== selfUrl) {
      try {
        await axios.get(`${leaderUrl}/internal/heartbeat`, { timeout: 2000 });
      } catch (e: any) {
        logger.warn("Leader heartbeat failed. Starting election.");
        await bully.startElection();
        if (bully.leaderId && bully.leaderId === nodeId) {
          isLeader = true;
          leaderUrl = selfUrl;
        }
      }
    }
  }, 5000);

});

