
export type MessageType =
  | "HEARTBEAT"
  | "ELECTION"
  | "ELECTION_OK"
  | "NEW_LEADER"
  | "REPL_OP"
  | "REPL_ACK"
  | "CLIENT_CREATE_VISIT"
  | "CLIENT_CLOSE_VISIT";

export interface BaseMessage {
  type: MessageType;
  fromId: number;
}

export interface HeartbeatMessage extends BaseMessage {
  type: "HEARTBEAT";
}

export interface ElectionMessage extends BaseMessage {
  type: "ELECTION";
}

export interface ElectionOkMessage extends BaseMessage {
  type: "ELECTION_OK";
}

export interface NewLeaderMessage extends BaseMessage {
  type: "NEW_LEADER";
  leaderId: number;
}

export interface ReplOpMessage extends BaseMessage {
  type: "REPL_OP";
  opId: number;
  op: any; 
}

export interface ReplAckMessage extends BaseMessage {
  type: "REPL_ACK";
  opId: number;
  ok: boolean;
}

export interface ClientCreateVisitMessage extends BaseMessage {
  type: "CLIENT_CREATE_VISIT";
  payload: {
    pacienteId: number;
    trabajadorSocialId: number;
    sala: string;
  };
}

export interface ClientCloseVisitMessage extends BaseMessage {
  type: "CLIENT_CLOSE_VISIT";
  payload: {
    visitId: number;
  };
}

export type Message =
  | HeartbeatMessage
  | ElectionMessage
  | ElectionOkMessage
  | NewLeaderMessage
  | ReplOpMessage
  | ReplAckMessage
  | ClientCreateVisitMessage
  | ClientCloseVisitMessage;
