import net from "node:net";
import fs from "node:fs";

const port = Number(process.argv[2]) || 4000;
const nodeId = process.argv[3] || "node1";

const leaderPort = 4000;
const isLeader = port === leaderPort;

const nodes = [
    { id: "node1", port: 4000 },
    { id: "node2", port: 4001 },
    { id: "node3", port: 4002 }
];

const DATA_FILE = `data-${nodeId}.json`;
const WAL_FILE = `wal-${nodeId}.log`;

const store = new Map<string, string>();

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

        else if (operation === "DELETE") {
            store.delete(key);
        }
    }
}

function sendToNode(port: number, command: string) {

    const socket = net.createConnection(
        { port },
        () => {
            socket.write(command + "\n");
        }
    );

    socket.on("data", () => {
        socket.end();
    });

    socket.on("error", () => {});
}

const server = net.createServer((socket) => {

    console.log(`Connection on ${nodeId}`);

    socket.on("data", (data) => {

        const command = data.toString().trim();

        if (!command) {
            socket.write("ERROR Empty command\n");
            return;
        }

        const parts = command.split(" ");

        const operation = parts[0].toUpperCase();
        const key = parts[1];
        const value = parts.slice(2).join(" ");

        // PUT
        if (operation === "PUT") {

            if (!isLeader) {
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

            for (const node of nodes) {

                if (node.id !== nodeId) {

                    sendToNode(
                        node.port,
                        `REPLICATE ${key} ${value}`
                    );
                }
            }

            socket.write("OK\n");
        }

        // REPLICATE
        else if (operation === "REPLICATE") {

            if (!key || !value) {
                socket.write(
                    "ERROR Invalid replication\n"
                );
                return;
            }

            store.set(key, value);

            writeToWAL(
                `PUT ${key} ${value}`
            );

            saveData();

            socket.write("OK\n");
        }

        // GET
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

        // DELETE
        else if (operation === "DELETE") {

            if (!isLeader) {
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

            for (const node of nodes) {

                if (node.id !== nodeId) {

                    sendToNode(
                        node.port,
                        `REPLICATE_DELETE ${key}`
                    );
                }
            }

            socket.write("OK\n");
        }

        // REPLICATE DELETE
        else if (operation === "REPLICATE_DELETE") {

            if (!key) {
                socket.write(
                    "ERROR Invalid replication\n"
                );
                return;
            }

            store.delete(key);

            writeToWAL(`DELETE ${key}`);
            saveData();

            socket.write("OK\n");
        }

        // PING
        else if (operation === "PING") {

            socket.write("PONG\n");
        }

        // QUIT
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

    socket.on("close", () => {
        console.log(`Connection closed on ${nodeId}`);
    });
});

loadData();
replayWAL();

server.listen(port, () => {

    console.log(
        `${nodeId} running on port ${port} ${
            isLeader ? "(LEADER)" : "(FOLLOWER)"
        }`
    );
});