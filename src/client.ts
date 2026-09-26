import net from "node:net";
import readline from "node:readline";

const port = Number(process.argv[2]) || 4000;

const client = net.createConnection(
    { port },
    () => {
        console.log(
            `Connected to node on port ${port}`
        );

        console.log(
            "Commands: PUT key value | GET key | DELETE key"
        );

        rl.prompt();
    }
);

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "> "
});

rl.on("line", (line) => {

    const command = line.trim();

    if (!command) {
        rl.prompt();
        return;
    }

    client.write(command + "\n");
});

client.on("data", (data) => {

    console.log(data.toString().trim());

    rl.prompt();
});

client.on("error", (err) => {

    console.log("Error:", err.message);
});

client.on("close", () => {

    console.log("Disconnected from server");

    process.exit(0);
});