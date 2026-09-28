import net from "node:net";
import fs from "node:fs";

const port = Number(process.argv[2]) || 4000;
const nodeId = process.argv[3] || "node1";

const nodes = [
    { id: "node1", port: 4000 },
    { id: "node2", port: 4001 },
    { id: "node3", port: 4002 }
];

const DATA_FILE = `data-${nodeId}.json`;
const RAFT_FILE = `raft-${nodeId}.json`;
const LOG_FILE = `log-${nodeId}.json`;

type LogEntry = {
    index: number;
    term: number;
    command: string;
};

const store = new Map<string, string>();
const raftLog: LogEntry[] = [];

let state = "follower";
let currentTerm = 0;
let votedFor: string | null = null;

let commitIndex = 0;
let lastApplied = 0;

let votesReceived = 0;
let electionTimer: NodeJS.Timeout;

function loadData() {
    if (!fs.existsSync(DATA_FILE)) return;

    const data = JSON.parse(
        fs.readFileSync(DATA_FILE, "utf-8")
    );

    for (const [key, value] of Object.entries(data)) {
        store.set(key, value as string);
    }
}

function saveData() {
    fs.writeFileSync(
        DATA_FILE,
        JSON.stringify(
            Object.fromEntries(store),
            null,
            2
        )
    );
}

function saveRaftState() {
    fs.writeFileSync(
        RAFT_FILE,
        JSON.stringify(
            {
                currentTerm,
                votedFor,
                commitIndex,
                lastApplied
            },
            null,
            2
        )
    );
}

function loadRaftState() {
    if (!fs.existsSync(RAFT_FILE)) return;

    const data = JSON.parse(
        fs.readFileSync(
            RAFT_FILE,
            "utf-8"
        )
    );

    currentTerm = data.currentTerm || 0;
    votedFor = data.votedFor || null;
    commitIndex = data.commitIndex || 0;
    lastApplied = data.lastApplied || 0;
}

function saveLog() {
    fs.writeFileSync(
        LOG_FILE,
        JSON.stringify(
            raftLog,
            null,
            2
        )
    );
}

function loadLog() {
    if (!fs.existsSync(LOG_FILE)) return;

    const data = JSON.parse(
        fs.readFileSync(
            LOG_FILE,
            "utf-8"
        )
    );

    raftLog.push(...data);
}

function applyCommand(command: string) {
    const parts = command.split(" ");

    const operation = parts[0];
    const key = parts[1];
    const value = parts.slice(2).join(" ");

    if (operation === "PUT") {
        store.set(key, value);
    }

    if (operation === "DELETE") {
        store.delete(key);
    }
}

function applyCommittedEntries() {
    while (lastApplied < commitIndex) {

        const entry = raftLog.find(
            e => e.index === lastApplied + 1
        );

        if (!entry) break;

        applyCommand(entry.command);

        lastApplied++;

        saveData();
        saveRaftState();
    }
}

function resetElectionTimer() {
    clearTimeout(electionTimer);

    const timeout =
        4000 + Math.random() * 3000;

    electionTimer = setTimeout(() => {

        if (state !== "leader") {
            startElection();
        }

    }, timeout);
}

function sendMessage(
    targetPort: number,
    message: string,
    callback?: (response: string) => void
) {
    const socket = net.createConnection(
        {
            port: targetPort
        },
        () => {
            socket.write(
                message + "\n"
            );
        }
    );

    let response = "";

    socket.on("data", data => {
        response += data.toString();
    });

    socket.on("end", () => {
        callback?.(
            response.trim()
        );
    });

    socket.on("error", () => {
        socket.destroy();
    });
}

function startElection() {

    if (state === "leader") {
        return;
    }

    state = "candidate";

    currentTerm++;

    votedFor = nodeId;

    votesReceived = 1;

    saveRaftState();

    console.log(
        `${nodeId} started election for term ${currentTerm}`
    );

    resetElectionTimer();

    for (const node of nodes) {

        if (node.id === nodeId) {
            continue;
        }

        sendMessage(
            node.port,
            `REQUEST_VOTE ${currentTerm} ${nodeId}`,
            response => {

                if (
                    state !== "candidate"
                ) {
                    return;
                }

                if (
                    response ===
                    "VOTE_GRANTED"
                ) {

                    votesReceived++;

                    const majority =
                        Math.floor(
                            nodes.length / 2
                        ) + 1;

                    if (
                        votesReceived >=
                        majority
                    ) {
                        becomeLeader();
                    }
                }
            }
        );
    }
}

function becomeLeader() {

    if (
        state === "leader"
    ) {
        return;
    }

    state = "leader";

    clearTimeout(
        electionTimer
    );

    console.log(
        `${nodeId} became LEADER for term ${currentTerm}`
    );

    sendHeartbeats();
}

function sendHeartbeats() {

    if (
        state !== "leader"
    ) {
        return;
    }

    for (const node of nodes) {

        if (node.id === nodeId) {
            continue;
        }

        sendMessage(
            node.port,
            `HEARTBEAT ${currentTerm} ${nodeId} ${commitIndex}`
        );
    }

    setTimeout(
        sendHeartbeats,
        1500
    );
}

function appendLogEntry(
    command: string
) {
    const entry: LogEntry = {
        index: raftLog.length + 1,
        term: currentTerm,
        command
    };

    raftLog.push(entry);

    saveLog();

    return entry;
}

function replicateEntry(
    entry: LogEntry,
    callback: (success: boolean) => void
) {

    let acknowledgements = 1;
    let completed = 0;
    let finished = false;

    const majority =
        Math.floor(
            nodes.length / 2
        ) + 1;

    for (const node of nodes) {

        if (node.id === nodeId) {
            continue;
        }

        sendMessage(
            node.port,
            `APPEND_ENTRY ${currentTerm} ${commitIndex} ${entry.index} ${entry.term} ${entry.command}`,
            response => {

                if (finished) {
                    return;
                }

                completed++;

                if (
                    response ===
                    "APPENDED"
                ) {
                    acknowledgements++;
                }

                if (
                    acknowledgements >=
                    majority
                ) {

                    finished = true;

                    callback(true);

                    return;
                }

                if (
                    completed ===
                    nodes.length - 1
                ) {

                    finished = true;

                    callback(false);
                }
            }
        );
    }
}

function commitEntry(
    entry: LogEntry
) {

    if (
        entry.index >
        commitIndex
    ) {

        commitIndex =
            entry.index;

        applyCommittedEntries();

        saveRaftState();
    }
}

const server = net.createServer(
    socket => {

        socket.on(
            "data",
            data => {

                const command =
                    data.toString().trim();

                if (!command) {
                    return;
                }

                const parts =
                    command.split(" ");

                const operation =
                    parts[0].toUpperCase();

                const key = parts[1];

                const value =
                    parts.slice(2).join(" ");

                if (
                    operation === "PUT"
                ) {

                    if (
                        state !== "leader"
                    ) {

                        socket.write(
                            "ERROR Not leader\n"
                        );

                        return;
                    }

                    if (
                        !key ||
                        !value
                    ) {

                        socket.write(
                            "ERROR Usage: PUT key value\n"
                        );

                        return;
                    }

                    const entry =
                        appendLogEntry(
                            command
                        );

                    replicateEntry(
                        entry,
                        success => {

                            if (!success) {

                                socket.write(
                                    "ERROR Majority not reached\n"
                                );

                                return;
                            }

                            commitEntry(
                                entry
                            );

                            socket.write(
                                `OK index=${entry.index} term=${entry.term}\n`
                            );
                        }
                    );
                }

                else if (
                    operation === "GET"
                ) {

                    if (!key) {

                        socket.write(
                            "ERROR Usage: GET key\n"
                        );

                        return;
                    }

                    const result =
                        store.get(key);

                    socket.write(
                        result === undefined
                            ? "NOT_FOUND\n"
                            : `${result}\n`
                    );
                }

                else if (
                    operation === "DELETE"
                ) {

                    if (
                        state !== "leader"
                    ) {

                        socket.write(
                            "ERROR Not leader\n"
                        );

                        return;
                    }

                    if (!key) {

                        socket.write(
                            "ERROR Usage: DELETE key\n"
                        );

                        return;
                    }

                    if (
                        !store.has(key)
                    ) {

                        socket.write(
                            "NOT_FOUND\n"
                        );

                        return;
                    }

                    const entry =
                        appendLogEntry(
                            command
                        );

                    replicateEntry(
                        entry,
                        success => {

                            if (!success) {

                                socket.write(
                                    "ERROR Majority not reached\n"
                                );

                                return;
                            }

                            commitEntry(
                                entry
                            );

                            socket.write(
                                `OK index=${entry.index} term=${entry.term}\n`
                            );
                        }
                    );
                }

                else if (
                    operation ===
                    "REQUEST_VOTE"
                ) {

                    const term =
                        Number(parts[1]);

                    const candidateId =
                        parts[2];

                    if (
                        term <
                        currentTerm
                    ) {

                        socket.write(
                            "VOTE_DENIED\n"
                        );

                        return;
                    }

                    if (
                        term >
                        currentTerm
                    ) {

                        currentTerm =
                            term;

                        state =
                            "follower";

                        votedFor =
                            null;

                        saveRaftState();
                    }

                    if (
                        votedFor === null ||
                        votedFor === candidateId
                    ) {

                        votedFor =
                            candidateId;

                        saveRaftState();

                        resetElectionTimer();

                        socket.write(
                            "VOTE_GRANTED\n"
                        );

                    } else {

                        socket.write(
                            "VOTE_DENIED\n"
                        );
                    }

                    socket.end();
                }

                else if (
                    operation ===
                    "HEARTBEAT"
                ) {

                    const term =
                        Number(parts[1]);

                    const leaderCommit =
                        Number(parts[3]);

                    if (
                        term <
                        currentTerm
                    ) {

                        socket.write(
                            "STALE\n"
                        );

                        socket.end();

                        return;
                    }

                    if (
                        term >
                        currentTerm
                    ) {

                        currentTerm =
                            term;

                        state =
                            "follower";

                        votedFor =
                            null;

                        saveRaftState();
                    }

                    state =
                        "follower";

                    if (
                        leaderCommit >
                        commitIndex
                    ) {

                        commitIndex =
                            Math.min(
                                leaderCommit,
                                raftLog.length
                            );

                        applyCommittedEntries();
                    }

                    resetElectionTimer();

                    socket.write(
                        "ALIVE\n"
                    );

                    socket.end();
                }

                else if (
                    operation ===
                    "APPEND_ENTRY"
                ) {

                    const term =
                        Number(parts[1]);

                    const leaderCommit =
                        Number(parts[2]);

                    const index =
                        Number(parts[3]);

                    const entryTerm =
                        Number(parts[4]);

                    const logCommand =
                        parts
                            .slice(5)
                            .join(" ");

                    if (
                        term <
                        currentTerm
                    ) {

                        socket.write(
                            "REJECTED\n"
                        );

                        socket.end();

                        return;
                    }

                    if (
                        term >
                        currentTerm
                    ) {

                        currentTerm =
                            term;

                        state =
                            "follower";

                        votedFor =
                            null;

                        saveRaftState();
                    }

                    state =
                        "follower";

                    resetElectionTimer();

                    const existing =
                        raftLog.find(
                            entry =>
                                entry.index ===
                                index
                        );

                    if (!existing) {

                        const entry: LogEntry = {
                            index,
                            term: entryTerm,
                            command:
                                logCommand
                        };

                        raftLog.push(
                            entry
                        );

                        saveLog();
                    }

                    if (
                        leaderCommit >
                        commitIndex
                    ) {

                        commitIndex =
                            Math.min(
                                leaderCommit,
                                raftLog.length
                            );

                        applyCommittedEntries();
                    }

                    socket.write(
                        "APPENDED\n"
                    );

                    socket.end();
                }

                else if (
                    operation === "PING"
                ) {

                    socket.write(
                        "PONG\n"
                    );

                    socket.end();
                }

                else if (
                    operation === "QUIT"
                ) {

                    socket.write(
                        "BYE\n"
                    );

                    socket.end();
                }

                else {

                    socket.write(
                        `ERROR Unknown command: ${operation}\n`
                    );

                    socket.end();
                }
            }
        );
    }
);

loadData();
loadRaftState();
loadLog();

if (
    commitIndex >
    raftLog.length
) {
    commitIndex =
        raftLog.length;
}

if (
    lastApplied >
    commitIndex
) {
    lastApplied =
        commitIndex;
}

applyCommittedEntries();

server.listen(
    port,
    () => {

        console.log(
            `${nodeId} running on port ${port}`
        );

        console.log(
            `Term: ${currentTerm}`
        );

        console.log(
            `VotedFor: ${votedFor}`
        );

        console.log(
            `Log entries: ${raftLog.length}`
        );

        console.log(
            `Commit index: ${commitIndex}`
        );

        resetElectionTimer();
    }
);