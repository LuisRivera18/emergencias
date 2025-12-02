// src/election.ts
import axios from "axios";
import winston from "winston";
const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

export class Bully {
  nodeId: number;
  peers: { id: number; url: string }[];
  leaderId: number | null = null;
  leaderUrl: string | null = null;

  constructor(nodeId: number, peers: { id: number; url: string }[]) {
    this.nodeId = nodeId;
    this.peers = peers;
  }

  async startElection() {
    // send to nodes with higher id
    const higher = this.peers.filter(p => p.id > this.nodeId);
    logger.info("Starting election, higher nodes:", higher.map(h => h.id));
    const responses = await Promise.all(
      higher.map(h => axios.post(`${h.url}/internal/election`, { from: this.nodeId }).then(r => true).catch(e => false))
    );
    const anyAlive = responses.some(r => r);
    if (!anyAlive) {
      // become leader
      logger.info("No higher nodes responded. Becoming leader.");
      this.leaderId = this.nodeId;
      this.leaderUrl = this.peers.find(p => p.id === this.nodeId)?.url || null;
      // notify all
      await Promise.all(this.peers.map(p => axios.post(`${p.url}/internal/leader`, { leaderId: this.nodeId, leaderUrl: this.leaderUrl }).catch(e => null)));
    } else {
      logger.info("Higher node exists; waiting for their leader announcement.");
      // wait for leader announcement (in real world implement timeout and retry)
    }
  }

  setLeader(id: number, url: string) {
    this.leaderId = id;
    this.leaderUrl = url;
  }
}
