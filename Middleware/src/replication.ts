// src/replication.ts
import axios from "axios";
import { DB } from "./db";
import winston from "winston";

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

export class Replicator {
  db: DB;
  peers: string[]; // base URLs of other nodes
  selfUrl: string;
  majority: number;

  constructor(db: DB, peers: string[], selfUrl: string) {
    this.db = db;
    this.peers = peers;
    this.selfUrl = selfUrl;
    this.majority = Math.floor(peers.length / 2) + 1;
  }

  // Op is a JSON object with {type: 'insert'|'update', table: '', data: {...}}
  async replicateOp(op: any) {
    // store in local WAL
    this.db.run("INSERT INTO WAL_LOG (op, applied) VALUES (?, 0)", [JSON.stringify(op)]);
    // send to peers and collect ACKs
    const urls = this.peers.map(p => `${p}/internal/replicate`);
    const promises = urls.map(u => axios.post(u, { op, from: this.selfUrl }).then(r => r.data).catch(e => null));
    const results = await Promise.all(promises);
    const acks = results.filter(r => r && r.ack).length;
    logger.info(`Replication: got ${acks} acks (need ${this.majority})`);
    if (acks >= this.majority) {
      // mark as applied locally and return success
      this.db.run("UPDATE WAL_LOG SET applied=1 WHERE op = ?", [JSON.stringify(op)]);
      return true;
    } else {
      // not enough ACKs
      return false;
    }
  }

  // endpoint receiver: apply op locally (used by peers)
  applyOpLocally(op: any) {
    // apply the op to local DB. support types used in app:
    const type = op.type;
    if (type === "assign_visit") {
      // op.data contains visitId, doctorId, camaId, folio, estado, consecutivo
      const d = op.data;
      try {
        this.db.transaction(() => {
          this.db.run(
            `UPDATE VISITAS SET doctor_id = ?, cama_id = ?, folio = ?, estado = ?, consecutivo = ? WHERE id = ?`,
            [d.doctor_id, d.cama_id, d.folio, d.estado, d.consecutivo, d.visit_id]
          );
          this.db.run(`UPDATE DOCTORES SET disponible = 0 WHERE id = ?`, [d.doctor_id]);
          this.db.run(`UPDATE CAMAS SET disponible = 0 WHERE id = ?`, [d.cama_id]);
        });
        return { ack: true };
      } catch (e) {
        logger.error("applyOpLocally failed", e);
        return { ack: false, error: e.message };
      }
    } else if (type === "create_visit") {
      const d = op.data;
      try {
        this.db.run(
          `INSERT INTO VISITAS (paciente_id, sala, trabajador_social_id, estado, consecutivo) VALUES (?, ?, ?, ?, ?)`,
          [d.paciente_id, d.sala, d.trabajador_social_id, d.estado || "ABIERTA", d.consecutivo || 0]
        );
        return { ack: true };
      } catch (e) {
        logger.error("applyOpLocally failed create_visit", e);
        return { ack: false, error: e.message };
      }
    } else if (type === "close_visit") {
      const d = op.data;
      try {
        this.db.transaction(() => {
          this.db.run(`UPDATE VISITAS SET estado='CERRADA', cerrado_en=CURRENT_TIMESTAMP WHERE id = ?`, [d.visit_id]);
          this.db.run(`UPDATE DOCTORES SET disponible = 1 WHERE id = ?`, [d.doctor_id]);
          this.db.run(`UPDATE CAMAS SET disponible = 1 WHERE id = ?`, [d.cama_id]);
        });
        return { ack: true };
      } catch (e) {
        logger.error("applyOpLocally failed close_visit", e);
        return { ack: false, error: e.message };
      }
    } else {
      return { ack: false, error: "unknown op" };
    }
  }
}
