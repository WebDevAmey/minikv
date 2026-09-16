import net from "node:net";

const store = new Map<string, string>();

const server = net.createServer((socket) => {
    socket.on("data", (data) => {
        const command = data.toString().trim();

        const parts = command.split(" ");

        const operation = parts[0];
        const key = parts[1];
        const value = parts.slice(2).join(" ");

        if (operation === "PUT") {
            store.set(key, value);
            socket.write("OK\n");
        }

        if (operation === "GET") {
            const result = store.get(key);

            socket.write(
                result === undefined
                    ? "NOT_FOUND\n"
                    : `${result}\n`
            );
        }

        if (operation === "DELETE") {
            const deleted = store.delete(key);

            socket.write(
                deleted
                    ? "OK\n"
                    : "NOT_FOUND\n"
            );
        }
    });
});

server.listen(4000, () => {
    console.log("KV server running on port 4000");
});