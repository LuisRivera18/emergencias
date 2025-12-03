// src/clusterNode.ts
import net from "net";
import { DB } from "./db";
import {
  Message,
  HeartbeatMessage,
  ElectionMessage,
  ElectionOkMessage,
  NewLeaderMessage,
  ReplOpMessage,
  ReplAckMessage,
  ClientCreateVisitMessage,
  ClientCloseVisitMessage
} from "./messages";

interface PeerInfo {
  id: number;
  host: string;
  tcpPort: number;
}

interface Config {
  nodeId: number;
  host: string;
  tcpPort: number;
  peers: PeerInfo[];
  initialLeader?: number;
}

type SocketWithBuffer = net.Socket & { buffer?: string };

export class ClusterNode {
  private config: Config;
  private db: DB;
  private server: net.Server;
  private peers: Map<number, SocketWithBuffer> = new Map();
  private isLeader = false;
  private leaderId: number | null = null;
  private nextOpId = 1;
  private lastHeartbeat = Date.now();

  constructor(config: Config, db: DB) {
    this.config = config;
    this.db = db;
    this.server = net.createServer((socket) => this.handleConnection(socket as SocketWithBuffer));
    this.init();
  }

  private init() {
    const { tcpPort, host, nodeId, peers, initialLeader } = this.config;

    this.server.listen(tcpPort, host, () => {
      console.log(`Node ${nodeId} listening on ${host}:${tcpPort}`);
    });

    // Conectar a peers
    peers.forEach((p) => {
      if (p.id !== nodeId) {
        this.connectToPeer(p);
      }
    });

    if (initialLeader && initialLeader === nodeId) {
      this.becomeLeader();
    } else if (initialLeader) {
      this.leaderId = initialLeader;
      this.lastHeartbeat = Date.now();
      console.log(Node ${nodeId} initial leader is Node ${initialLeader});
    }

    // Heartbeats (en el líder)
    setInterval(() => this.sendHeartbeat(), 3000);

    // Checar si el líder murió (en followers)
    setInterval(() => this.checkLeaderTimeout(), 5000);
  }

  private connectToPeer(peer: PeerInfo) {
    const { nodeId } = this.config;
    const socket = net.createConnection(peer.tcpPort, peer.host, () => {
      console.log(`Node ${nodeId} connected to peer ${peer.id}`);
      (socket as SocketWithBuffer).buffer = "";
      this.peers.set(peer.id, socket as SocketWithBuffer);
    });

    socket.on("error", () => {
      // silencioso
    });

    socket.on("close", () => {
      console.log(`Connection to peer ${peer.id} closed`);
      this.peers.delete(peer.id);
      setTimeout(() => this.connectToPeer(peer), 3000);
    });

    socket.on("data", (data) => {
      const sock = socket as SocketWithBuffer;
      sock.buffer = (sock.buffer || "") + data.toString("utf8");
      let idx;
      while ((idx = sock.buffer.indexOf("\n")) >= 0) {
        const raw = sock.buffer.slice(0, idx);
        sock.buffer = sock.buffer.slice(idx + 1);
        if (raw.trim().length === 0) continue;
        try {
          const msg = JSON.parse(raw) as Message;
          this.handleMessage(msg, peer.id);
        } catch (e: any) {
          console.error("Invalid message from peer", e.message);
        }
      }
    });
  }

  private handleConnection(socket: SocketWithBuffer) {
    socket.buffer = "";
    socket.on("data", (data) => {
      socket.buffer = (socket.buffer || "") + data.toString("utf8");
      let idx;
      while ((idx = socket.buffer.indexOf("\n")) >= 0) {
        const raw = socket.buffer.slice(0, idx);
        socket.buffer = socket.buffer.slice(idx + 1);
        if (raw.trim().length === 0) continue;
        try {
          const msg = JSON.parse(raw) as Message;
          this.handleMessage(msg, msg.fromId);
        } catch (e: any) {
          console.error("Invalid message (server side)", e.message);
        }
      }
    });
  }

  private sendToPeer(peerId: number, msg: Message) {
    const sock = this.peers.get(peerId);
    if (!sock) return;
    sock.write(JSON.stringify(msg) + "\n");
  }

  private broadcast(msg: Message) {
    for (const peerId of this.peers.keys()) {
      this.sendToPeer(peerId, msg);
    }
  }

  private sendHeartbeat() {
    if (!this.isLeader) return;
    const msg: HeartbeatMessage = {
      type: "HEARTBEAT",
      fromId: this.config.nodeId
    };
    this.broadcast(msg);
  }

  private checkLeaderTimeout() {
    if (this.isLeader) return; // yo soy el líder
    if (!this.leaderId) {
      // no hay líder conocido, lanzo elección
      this.startElection();
      return;
    }
    const elapsed = Date.now() - this.lastHeartbeat;
    if (elapsed > 10000) {
      console.log(`Node ${this.config.nodeId} detected leader timeout (${elapsed}ms). Starting election...`);
      this.leaderId = null;
      this.startElection();
    }
  }

  private startElection() {
    console.log(`Node ${this.config.nodeId} starting election...`);
    const higherPeers = this.config.peers.filter((p) => p.id > this.config.nodeId);
    if (higherPeers.length === 0) {
      this.becomeLeader();
      return;
    }

    const msg: ElectionMessage = {
      type: "ELECTION",
      fromId: this.config.nodeId
    };

    higherPeers.forEach((p) => this.sendToPeer(p.id, msg));
  }

  private becomeLeader() {
    this.isLeader = true;
    this.leaderId = this.config.nodeId;
    this.lastHeartbeat = Date.now();
    console.log(`Node ${this.config.nodeId} became LEADER`);
    const msg: NewLeaderMessage = {
      type: "NEW_LEADER",
      fromId: this.config.nodeId,
      leaderId: this.config.nodeId
    };
    this.broadcast(msg);
  }

  private handleMessage(msg: Message, fromPeerId: number) {
    switch (msg.type) {
      case "HEARTBEAT":
        if (this.leaderId !== msg.fromId) {
          this.leaderId = msg.fromId;
          console.log(`Node ${this.config.nodeId} now recognizes leader Node ${msg.fromId}`);
        }
        this.lastHeartbeat = Date.now();
        break;

      case "ELECTION": {
        if (this.config.nodeId > msg.fromId) {
          const reply: ElectionOkMessage = {
            type: "ELECTION_OK",
            fromId: this.config.nodeId
          };
          this.sendToPeer(msg.fromId, reply);
          this.startElection();
        }
        break;
      }

      case "ELECTION_OK":
        // En una implementación completa habría lógica para esperar NEW_LEADER
        break;

      case "NEW_LEADER":
        this.isLeader = msg.leaderId === this.config.nodeId;
        this.leaderId = msg.leaderId;
        this.lastHeartbeat = Date.now();
        console.log(`Node ${this.config.nodeId} knows new leader: Node ${msg.leaderId}`);
        break;

      case "REPL_OP":
        this.applyReplicationOp(msg as ReplOpMessage);
        break;

      case "REPL_ACK":
        // (Simplificado) Podrías contar ACKs aquí si quieres super-consenso explícito
        break;

      case "CLIENT_CREATE_VISIT":
        this.handleClientCreateVisit(msg as ClientCreateVisitMessage);
        break;

      case "CLIENT_CLOSE_VISIT":
        this.handleClientCloseVisit(msg as ClientCloseVisitMessage);
        break;
    }
  }

  // ===== API para CLI =====

  public clientCreateVisit(pacienteId: number, trabajadorId: number, sala: string) {
    if (this.isLeader) {
      this.createVisitAsLeader(pacienteId, trabajadorId, sala);
    } else if (this.leaderId) {
      const msg: ClientCreateVisitMessage = {
        type: "CLIENT_CREATE_VISIT",
        fromId: this.config.nodeId,
        payload: { pacienteId, trabajadorSocialId: trabajadorId, sala }
      };
      this.sendToPeer(this.leaderId, msg);
      console.log(`Node ${this.config.nodeId} forwarded visit creation to leader ${this.leaderId}`);
    } else {
      console.log("No leader known, cannot create visit");
    }
  }

  public clientCloseVisit(visitId: number) {
    if (this.isLeader) {
      this.closeVisitAsLeader(visitId);
    } else if (this.leaderId) {
      const msg: ClientCloseVisitMessage = {
        type: "CLIENT_CLOSE_VISIT",
        fromId: this.config.nodeId,
        payload: { visitId }
      };
      this.sendToPeer(this.leaderId, msg);
      console.log(`Node ${this.config.nodeId} forwarded close-visit to leader ${this.leaderId}`);
    } else {
      console.log("No leader known, cannot close visit");
    }
  }

  // ===== Handlers en el líder =====

  private handleClientCreateVisit(msg: ClientCreateVisitMessage) {
    if (!this.isLeader) return;
    const { pacienteId, trabajadorSocialId, sala } = msg.payload;
    this.createVisitAsLeader(pacienteId, trabajadorSocialId, sala);
  }

  private handleClientCloseVisit(msg: ClientCloseVisitMessage) {
    if (!this.isLeader) return;
    this.closeVisitAsLeader(msg.payload.visitId);
  }

  // ===== Lógica de negocio =====

  private createVisitAsLeader(pacienteId: number, trabajadorId: number, sala: string) {
    const doctor = this.db.get("SELECT id FROM DOCTORES WHERE disponible = 1 LIMIT 1");
    const cama = this.db.get("SELECT id FROM CAMAS WHERE disponible = 1 LIMIT 1");
    if (!doctor || !cama) {
      console.log("No hay recursos disponibles (doctor/cama)");
      return;
    }

    const insertResult = this.db.run(
      "INSERT INTO VISITAS (paciente_id, sala, trabajador_social_id, estado, consecutivo) VALUES (?, ?, ?, 'ABIERTA', 0)",
      [pacienteId, sala, trabajadorId]
    );
    const visitId = Number(insertResult.lastInsertRowid);

    const cntRow = this.db.get("SELECT COUNT(*) as c FROM VISITAS WHERE sala = ?", [sala]);
    const consecutivo = (cntRow && cntRow.c) || 1;
    const folio = `${pacienteId}-${doctor.id}-${sala}-${consecutivo}`;

    this.db.transaction(() => {
      this.db.run(
        "UPDATE VISITAS SET doctor_id = ?, cama_id = ?, folio = ?, estado = 'ASIGNADA', consecutivo = ? WHERE id = ?",
        [doctor.id, cama.id, folio, consecutivo, visitId]
      );
      this.db.run("UPDATE DOCTORES SET disponible = 0 WHERE id = ?", [doctor.id]);
      this.db.run("UPDATE CAMAS SET disponible = 0 WHERE id = ?", [cama.id]);
      this.db.run(
        "INSERT INTO WAL_LOG (op_type, payload) VALUES (?, ?)",
        ["assign_visit", JSON.stringify({ visitId, pacienteId, trabajadorId, sala, doctorId: doctor.id, camaId: cama.id, folio, consecutivo })]
      );
    });

    console.log(`
      Leader ${this.config.nodeId} created visit ${visitId}, doctor=${doctor.id}, cama=${cama.id}, folio=${folio}
    `);

    const op: ReplOpMessage = {
      type: "REPL_OP",
      fromId: this.config.nodeId,
      opId: this.nextOpId++,
      op: {
        kind: "assign_visit",
        data: {
          visitId,
          doctorId: doctor.id,
          camaId: cama.id,
          folio,
          estado: "ASIGNADA",
          consecutivo,
          pacienteId,
          trabajadorId,
          sala
        }
      }
    };
    this.broadcast(op);
  }

  private closeVisitAsLeader(visitId: number) {
    const v = this.db.get("SELECT * FROM VISITAS WHERE id = ?", [visitId]);
    if (!v) {
      console.log(`Visita ${visitId} no existe`);
      return;
    }
    if (v.estado === "CERRADA") {
      console.log(`Visita ${visitId} ya estaba cerrada`);
      return;
    }

    this.db.transaction(() => {
      this.db.run("UPDATE VISITAS SET estado='CERRADA', cerrado_en=CURRENT_TIMESTAMP WHERE id = ?", [visitId]);
      if (v.doctor_id) this.db.run("UPDATE DOCTORES SET disponible = 1 WHERE id = ?", [v.doctor_id]);
      if (v.cama_id) this.db.run("UPDATE CAMAS SET disponible = 1 WHERE id = ?", [v.cama_id]);
      this.db.run(
        "INSERT INTO WAL_LOG (op_type, payload) VALUES (?, ?)",
        ["close_visit", JSON.stringify({ visitId, doctorId: v.doctor_id, camaId: v.cama_id })]
      );
    });

    console.log(`Leader ${this.config.nodeId} closed visit ${visitId}`);

    const op: ReplOpMessage = {
      type: "REPL_OP",
      fromId: this.config.nodeId,
      opId: this.nextOpId++,
      op: {
        kind: "close_visit",
        data: {
          visitId,
          doctorId: v.doctor_id,
          camaId: v.cama_id
        }
      }
    };
    this.broadcast(op);
  }

  private applyReplicationOp(msg: ReplOpMessage) {
    const { kind, data } = msg.op;

    if (kind === "assign_visit") {
      const d = data;
      this.db.transaction(() => {
        const v = this.db.get("SELECT * FROM VISITAS WHERE id = ?", [d.visitId]);
        if (!v) {
          this.db.run(
            "INSERT INTO VISITAS (id, paciente_id, sala, trabajador_social_id, estado, doctor_id, cama_id, folio, consecutivo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
              d.visitId,
              d.pacienteId,
              d.sala,
              d.trabajadorId,
              d.estado,
              d.doctorId,
              d.camaId,
              d.folio,
              d.consecutivo
            ]
          );
        } else {
          this.db.run(
            "UPDATE VISITAS SET doctor_id = ?, cama_id = ?, folio = ?, estado = ?, consecutivo = ? WHERE id = ?",
            [d.doctorId, d.camaId, d.folio, d.estado, d.consecutivo, d.visitId]
          );
        }

        this.db.run("UPDATE DOCTORES SET disponible = 0 WHERE id = ?", [d.doctorId]);
        this.db.run("UPDATE CAMAS SET disponible = 0 WHERE id = ?", [d.camaId]);
      });
    } else if (kind === "close_visit") {
      const d = data;
      this.db.transaction(() => {
        this.db.run("UPDATE VISITAS SET estado='CERRADA', cerrado_en=CURRENT_TIMESTAMP WHERE id = ?", [d.visitId]);
        if (d.doctorId) this.db.run("UPDATE DOCTORES SET disponible = 1 WHERE id = ?", [d.doctorId]);
        if (d.camaId) this.db.run("UPDATE CAMAS SET disponible = 1 WHERE id = ?", [d.camaId]);
      });
    }

    const ack: ReplAckMessage = {
      type: "REPL_ACK",
      fromId: this.config.nodeId,
      opId: msg.opId,
      ok: true
    };
    this.sendToPeer(msg.fromId, ack);
  }

  public listVisits() {
    const rows = this.db.all("SELECT * FROM VISITAS ORDER BY creado_en DESC");
    console.table(rows);
  }
}
