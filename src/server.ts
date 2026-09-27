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
const WAL_FILE = `wal-${nodeId}.log`;
const RAFT_FILE = `raft-${nodeId}.json`;

const store = new Map<string, string>();

let state = "follower";
let currentTerm = 0;
let votedFor: string | null = null;
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
        JSON.stringify(Object.fromEntries(store), null, 2)
    );
}

function writeToWAL(command: string) {
    fs.appendFileSync(WAL_FILE, command + "\n");
}

function replayWAL() {
    if (!fs.existsSync(WAL_FILE)) return;

    const logs = fs.readFileSync(WAL_FILE, "utf-8")
        .split("\n")
        .filter(line => line.trim());

    for (const command of logs) {
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
}

function saveRaftState() {
    fs.writeFileSync(
        RAFT_FILE,
        JSON.stringify(
            {
                currentTerm,
                votedFor
            },
            null,
            2
        )
    );
}

function loadRaftState() {
    if (!fs.existsSync(RAFT_FILE)) return;

    const data = JSON.parse(
        fs.readFileSync(RAFT_FILE, "utf-8")
    );

    currentTerm = data.currentTerm || 0;
    votedFor = data.votedFor || null;
}

function resetElectionTimer() {
    clearTimeout(electionTimer);

    const timeout = 4000 + Math.random() * 3000;

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
        { port: targetPort },
        () => {
            socket.write(message + "\n");
        }
    );

    let response = "";

    socket.on("data", (data) => {
        response += data.toString();
    });

    socket.on("end", () => {
        callback?.(response.trim());
    });

    socket.on("error", () => {
        socket.destroy();
    });
}

function startElection() {
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
        if (node.id === nodeId) continue;

        sendMessage(
            node.port,
            `REQUEST_VOTE ${currentTerm} ${nodeId}`,
            (response) => {
                if (
                    response === "VOTE_GRANTED" &&
                    state === "candidate"
                ) {
                    votesReceived++;

                    const majority =
                        Math.floor(nodes.length / 2) + 1;

                    if (votesReceived >= majority) {
                        becomeLeader();
                    }
                }
            }
        );
    }
}

function becomeLeader() {
    state = "leader";

    clearTimeout(electionTimer);

    console.log(
        `${nodeId} became LEADER for term ${currentTerm}`
    );

    sendHeartbeats();
}

function sendHeartbeats() {
    if (state !== "leader") return;

    for (const node of nodes) {
        if (node.id === nodeId) continue;

        sendMessage(
            node.port,
            `HEARTBEAT ${currentTerm} ${nodeId}`
        );
    }

    setTimeout(sendHeartbeats, 2000);
}

function replicate(command: string) {
    for (const node of nodes) {
        if (node.id === nodeId) continue;

        sendMessage(
            node.port,
            `REPLICATE ${command}`
        );
    }
}

const server = net.createServer((socket) => {

    socket.on("data", (data) => {

        const command = data.toString().trim();

        if (!command) return;

        const parts = command.split(" ");

        const operation = parts[0].toUpperCase();
        const key = parts[1];
        const value = parts.slice(2).join(" ");

        if (operation === "PUT") {

            if (state !== "leader") {
                socket.write("ERROR Not leader\n");
                return;
            }

            if (!key || !value) {
                socket.write(
                    "ERROR Usage: PUT key value\n"
                );
                return;
            }

            writeToWAL(command);

            store.set(key, value);

            saveData();

            replicate(command);

            socket.write("OK\n");
        }

        else if (operation === "GET") {

            if (!key) {
                socket.write(
                    "ERROR Usage: GET key\n"
                );
                return;
            }

            const result = store.get(key);

            socket.write(
                result === undefined
                    ? "NOT_FOUND\n"
                    : `${result}\n`
            );
        }

        else if (operation === "DELETE") {

            if (state !== "leader") {
                socket.write("ERROR Not leader\n");
                return;
            }

            if (!key) {
                socket.write(
                    "ERROR Usage: DELETE key\n"
                );
                return;
            }

            const deleted = store.delete(key);

            if (!deleted) {
                socket.write("NOT_FOUND\n");
                return;
            }

            writeToWAL(command);

            saveData();

            replicate(command);

            socket.write("OK\n");
        }

        else if (operation === "REQUEST_VOTE") {

            const term = Number(parts[1]);
            const candidateId = parts[2];

            if (term < currentTerm) {
                socket.write("VOTE_DENIED\n");
                return;
            }

            if (term > currentTerm) {

                currentTerm = term;
                state = "follower";
                votedFor = null;

                saveRaftState();
            }

            if (
                votedFor === null ||
                votedFor === candidateId
            ) {

                votedFor = candidateId;

                saveRaftState();

                resetElectionTimer();

                socket.write("VOTE_GRANTED\n");

            } else {

                socket.write("VOTE_DENIED\n");
            }
        }

        else if (operation === "HEARTBEAT") {

            const term = Number(parts[1]);

            if (term < currentTerm) {
                socket.write("STALE\n");
                return;
            }

            if (term > currentTerm) {

                currentTerm = term;
                state = "follower";
                votedFor = null;

                saveRaftState();
            }

            state = "follower";

            resetElectionTimer();

            socket.write("ALIVE\n");
        }

        else if (operation === "REPLICATE") {

            const replicatedCommand =
                parts.slice(1).join(" ");

            const replicatedParts =
                replicatedCommand.split(" ");

            const replicatedOperation =
                replicatedParts[0];

            const replicatedKey =
                replicatedParts[1];

            const replicatedValue =
                replicatedParts.slice(2).join(" ");

            if (replicatedOperation === "PUT") {

                store.set(
                    replicatedKey,
                    replicatedValue
                );

                writeToWAL(replicatedCommand);

                saveData();
            }

            else if (
                replicatedOperation === "DELETE"
            ) {

                store.delete(replicatedKey);

                writeToWAL(replicatedCommand);

                saveData();
            }

            socket.write("OK\n");
        }

        else if (operation === "PING") {

            socket.write("PONG\n");
        }

        else if (operation === "QUIT") {

            socket.write("BYE\n");

            socket.end();
        }

        else {

            socket.write(
                `ERROR Unknown command: ${operation}\n`
            );
        }
    });
});

loadData();
loadRaftState();
replayWAL();

server.listen(port, () => {

    console.log(
        `${nodeId} running on port ${port}`
    );

    console.log(
        `Term: ${currentTerm}, VotedFor: ${votedFor}`
    );

    resetElectionTimer();
});