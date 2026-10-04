const { spawn } = require("child_process");

function runSherlock(username, { signal, onOutput } = {}) {

    return new Promise((resolve, reject) => {

        console.log(`[SHERLOCK] Starting search for: ${username}`);

        let output = "";
        let errorOutput = "";
        let settled = false;
        let child;

        const finish = (error, result) => {
            if (settled) {
                return;
            }

            settled = true;
            signal?.removeEventListener("abort", onAbort);

            if (error) {
                reject(error);
            } else {
                resolve(result);
            }
        };

        const onAbort = () => {
            const error = new Error(
                signal.reason || "Sherlock search cancelled"
            );
            error.name = "AbortError";

            if (!child) {
                finish(error);
                return;
            }

            child.kill();
            finish(error);
        };

        if (signal?.aborted) {
            onAbort();
            return;
        }

        try {
            child = spawn(
                "python",
                [
                    "-m",
                    "sherlock_project",
                    username,
                    "--print-found"
                ]
            );
        } catch (error) {
            finish(error);
            return;
        }

        signal?.addEventListener("abort", onAbort, { once: true });

        child.stdout.on("data", (data) => {

            const text = data.toString();

            output += text;

            console.log(`[SHERLOCK] ${text.trim()}`);

            onOutput?.(text);
        });

        child.stderr.on("data", (data) => {

            const text = data.toString();

            errorOutput += text;

            console.log(`[SHERLOCK ERROR] ${text.trim()}`);
        });

        child.on("error", (error) => {

            console.log(
                `[SHERLOCK PROCESS ERROR] ${error.message}`
            );

            finish(error);
        });

        child.on("close", (code) => {

            console.log(
                `[SHERLOCK] Process finished with code: ${code}`
            );

            if (code !== 0) {

                return finish(
                    new Error(
                        errorOutput ||
                        `Sherlock exited with code ${code}`
                    )
                );
            }

            finish(null, output);
        });
    });
}

module.exports = runSherlock;