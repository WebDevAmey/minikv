import net from "node:net";
import fs from "node:fs";

const store = new Map<string, string>();

const DATA_FILE = "data.json";
const WAL_FILE = "wal.log";

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
    const data = Object.fromEntries(store);

    fs.writeFileSync(
        DATA_FILE,
        JSON.stringify(data, null, 2)
    );
}

function writeToWAL(command: string) {
    fs.appendFileSync(WAL_FILE, command + "\n");
}

function replayWAL() {
    if (!fs.existsSync(WAL_FILE)) return;

    const logs = fs.readFileSync(WAL_FILE, "utf-8")
        .split("\n")
        .filter(line => line.trim() !== "");

    for (const command of logs) {
        const parts = command.split(" ");

        const operation = parts[0];
        const key = parts[1];
        const value = parts.slice(2).join(" ");

        if (operation === "PUT") {
            store.set(key, value);
        } else if (operation === "DELETE") {
            store.delete(key);
        }
    }
}

const server = net.createServer((socket) => {

    console.log("Client connected");

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

        if (operation === "PUT") {

            if (!key || !value) {
                socket.write("ERROR Usage: PUT key value\n");
                return;
            }

            writeToWAL(command);
            store.set(key, value);
            saveData();

            socket.write("OK\n");

        } else if (operation === "GET") {

            if (!key) {
                socket.write("ERROR Usage: GET key\n");
                return;
            }

            const result = store.get(key);

            socket.write(
                result === undefined
                    ? "NOT_FOUND\n"
                    : `${result}\n`
            );

        } else if (operation === "DELETE") {

            if (!key) {
                socket.write("ERROR Usage: DELETE key\n");
                return;
            }

            const deleted = store.delete(key);

            if (deleted) {
                writeToWAL(command);
                saveData();
                socket.write("OK\n");
            } else {
                socket.write("NOT_FOUND\n");
            }

        } else if (operation === "PING") {

            socket.write("PONG\n");

        } else if (operation === "QUIT") {

            socket.write("BYE\n");
            socket.end();

        } else {

            socket.write(
                `ERROR Unknown command: ${operation}\n`
            );
        }
    });

    socket.on("close", () => {
        console.log("Client disconnected");
    });
});

loadData();
replayWAL();

server.listen(4000, () => {
    console.log("KV server running on port 4000");
});