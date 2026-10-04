const express = require("express");
const cors = require("cors");
const path = require("path");
const http = require("http");

const { Server } = require("socket.io");
const { QueueEvents } = require("bullmq");

const detectTarget = require("./utils/target_detector");

const {
    investigationQueue,
    connection
} = require("./queue/investigationQueue");

const app = express();

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*"
    }
});

const investigationWorker =
    require("./workers/investigationWorker");

investigationWorker.setSocketIsConnected(
    (socketId) => io.sockets.sockets.has(socketId)
);

const PORT = 3000;


// =================================
// WEBSOCKET
// =================================

io.on("connection", (socket) => {

    console.log(
        `[WEBSOCKET] Client connected: ${socket.id}`
    );


    // =================================
    // ATTACH JOB TO SOCKET
    // =================================

    socket.on("investigation:start", (jobId) => {

        if (!socket.jobIds) {
            socket.jobIds = new Set();
        }

        socket.jobIds.add(String(jobId));

        console.log(
            `[WEBSOCKET] Socket ${socket.id} attached to Job ${jobId}`
        );
    });


    // =================================
    // SOCKET DISCONNECTED
    // =================================

    socket.on("disconnect", async () => {

        console.log(
            `[WEBSOCKET] Client disconnected: ${socket.id}`
        );


        const jobIds = socket.jobIds || new Set();

        // Keep compatibility with sockets attached through the legacy event.
        if (socket.jobId) {
            jobIds.add(String(socket.jobId));
        }

        if (jobIds.size === 0) {

            console.log(
                "[WEBSOCKET] No job attached to socket"
            );

            return;
        }


        for (const jobId of jobIds) {
            try {

                const job =
                    await investigationQueue.getJob(jobId);


                // Job doesn't exist
                if (!job) {

                    console.log(
                        `[QUEUE] Job ${jobId} does not exist`
                    );

                    continue;
                }


                const state =
                    await job.getState();


                console.log(
                    `[QUEUE] Job ${jobId} state: ${state}`
                );


                // =================================
                // REMOVE JOBS THAT HAVE NOT STARTED
                // =================================

                if (
                    state === "waiting" ||
                    state === "delayed" ||
                    state === "paused" ||
                    state === "prioritized"
                ) {

                    await job.remove();

                    console.log(
                        `[QUEUE] Job ${jobId} removed because client disconnected`
                    );

                    continue;
                }


                // =================================
                // ACTIVE JOB
                // =================================

                if (state === "active") {

                    const cancelled =
                        investigationWorker.cancelJobForDisconnectedSocket(
                            jobId,
                            "Client socket disconnected"
                        );

                    console.log(
                        cancelled
                            ? `[QUEUE] Active job ${jobId} cancellation requested`
                            : `[QUEUE] Active job ${jobId} is not tracked by this worker`
                    );

                    continue;
                }


                // =================================
                // OTHER STATES
                // =================================

                console.log(
                    `[QUEUE] Job ${jobId} is already ${state}`
                );

            } catch (error) {

                console.error(
                    `[QUEUE] Failed to handle disconnected job ${jobId}:`,
                    error.message
                );
            }
        }

        jobIds.clear();
    });
});


// =================================
// BULLMQ QUEUE EVENTS
// =================================

const queueEvents = new QueueEvents(
    "investigation",
    {
        connection
    }
);

investigationQueue.on("error", (error) => {
    console.error(`[QUEUE] Queue error: ${error.message}`);
});

queueEvents.on("error", (error) => {
    console.error(`[QUEUE] Queue events error: ${error.message}`);
});


// =================================
// JOB PROGRESS
// =================================

queueEvents.on(
    "progress",
    async ({ jobId, data }) => {
        try {
            const job = await investigationQueue.getJob(jobId);
            const socketId = job?.data?.socketId;

            if (!socketId) {
                return;
            }

            if (typeof data === "number") {
                console.log(
                    `[QUEUE] Job ${jobId} progress: ${data}`
                );

                io.to(socketId).emit(
                    "investigation:progress",
                    {
                        jobId,
                        progress: data
                    }
                );
                return;
            }

            if (data?.output) {
                io.to(socketId).emit(
                    "investigation:output",
                    {
                        jobId,
                        output: data.output
                    }
                );
            }
        } catch (error) {
            console.error(
                `[QUEUE] Failed to forward progress for job ${jobId}: ${error.message}`
            );
        }
    }
);


// =================================
// JOB COMPLETED
// =================================

queueEvents.on(
    "completed",
    async ({ jobId, returnvalue }) => {
        try {
            const job = await investigationQueue.getJob(jobId);
            const socketId = job?.data?.socketId;

            console.log(
                `[QUEUE] Job ${jobId} completed`
            );

            if (socketId) {
                io.to(socketId).emit(
                    "investigation:completed",
                    {
                        jobId,
                        result: returnvalue
                    }
                );
            }
        } catch (error) {
            console.error(
                `[QUEUE] Failed to forward result for job ${jobId}: ${error.message}`
            );
        }
    }
);


// =================================
// JOB FAILED
// =================================

queueEvents.on(
    "failed",
    async ({ jobId, failedReason }) => {
        try {
            const job = await investigationQueue.getJob(jobId);
            const socketId = job?.data?.socketId;

            console.log(
                `[QUEUE] Job ${jobId} failed: ${failedReason}`
            );

            if (socketId) {
                io.to(socketId).emit(
                    "investigation:failed",
                    {
                        jobId,
                        error: failedReason
                    }
                );
            }
        } catch (error) {
            console.error(
                `[QUEUE] Failed to forward failure for job ${jobId}: ${error.message}`
            );
        }
    }
);


// =================================
// MIDDLEWARE
// =================================

app.use(cors());

app.use(express.json());

app.use(
    express.static(
        path.join(__dirname, "../public")
    )
);


// =================================
// SERVER STARTUP
// =================================

console.log("=================================");
console.log("       OSINT-X BACKEND");
console.log("=================================");


// =================================
// HEALTH CHECK
// =================================

app.get("/api/health", (req, res) => {

    console.log(
        "[HEALTH] Health check received"
    );

    res.json({
        status: "ok",
        message: "OSINT-X backend is running"
    });
});


// =================================
// CREATE INVESTIGATION
// =================================

app.post(
    "/api/investigations",
    async (req, res) => {

        console.log("---------------------------------");
        console.log("[INVESTIGATION] New request");

        const name =
            typeof req.body?.name === "string"
                ? req.body.name.trim()
                : "";

        const target =
            typeof req.body?.target === "string"
                ? req.body.target.trim()
                : "";
        const socketId =
            typeof req.body?.socketId === "string"
                ? req.body.socketId
                : null;


        console.log(
            "[NAME]   :",
            name
        );

        console.log(
            "[TARGET] :",
            target
        );


        // =================================
        // VALIDATION
        // =================================

        if (!name || !target) {

            console.log(
                "[ERROR] Missing data"
            );

            return res.status(400).json({
                success: false,
                error:
                    "Name and target are required"
            });
        }

        if (!socketId || !io.sockets.sockets.has(socketId)) {
            return res.status(409).json({
                success: false,
                error: "A connected WebSocket is required to start an investigation"
            });
        }


        // =================================
        // TARGET DETECTION
        // =================================

        const detected =
            detectTarget(target);

        if (detected.type !== "username") {
            return res.status(422).json({
                success: false,
                error: "Only username investigations are supported"
            });
        }


        console.log(
            "[TARGET DETECTOR]"
        );

        console.log(
            "[TYPE] :",
            detected.type
        );

        console.log(
            "[VALUE]:",
            detected.value
        );


        // =================================
        // CREATE BULLMQ JOB
        // =================================

        const job =
            await investigationQueue.add(
                "investigation",
                {
                    name,
                    target: detected.value,
                    targetType: detected.type,
                    socketId
                }
            );

        {
            const socket = io.sockets.sockets.get(socketId);

            if (!socket) {
                const state = await job.getState();

                if (
                    state === "waiting" ||
                    state === "delayed" ||
                    state === "paused" ||
                    state === "prioritized"
                ) {
                    await job.remove();
                    console.log(
                        `[QUEUE] Job ${job.id} removed because socket ${socketId} disconnected before attachment`
                    );
                } else if (state === "active") {
                    investigationWorker.cancelJobForDisconnectedSocket(
                        String(job.id),
                        "Client socket disconnected before attachment"
                    );
                }

                return res.status(409).json({
                    success: false,
                    error: "Socket disconnected before the investigation could be attached"
                });
            }

            if (!socket.jobIds) {
                socket.jobIds = new Set();
            }

            socket.jobIds.add(String(job.id));
        }


        console.log(
            `[QUEUE] Job created: ${job.id}`
        );


        // =================================
        // RESPONSE
        // =================================

        res.status(202).json({

            success: true,

            message:
                "Investigation started",

            jobId: job.id,

            investigation: {

                name,

                target:
                    detected.value,

                type:
                    detected.type,

                status:
                    "queued"
            }
        });
    }
);


// =================================
// START SERVER
// =================================

async function startServer() {
    try {
        await investigationQueue.waitUntilReady();

        const pendingJobs =
            await investigationQueue.getJobs(
                ["waiting", "delayed", "paused", "prioritized"],
                0,
                -1
            );

        for (const job of pendingJobs) {
            const socketId = job.data?.socketId;

            if (!socketId || !io.sockets.sockets.has(socketId)) {
                await job.remove();
                console.log(
                    `[QUEUE] Removed pending job ${job.id} because its socket is disconnected`
                );
            }
        }

        investigationWorker.run().catch((error) => {
            console.error(
                `[WORKER] Failed to start: ${error.message}`
            );
            process.exitCode = 1;
        });

        server.listen(
            PORT,
            () => {
                console.log("[SERVER] OSINT-X running");
                console.log(`[SERVER] http://localhost:${PORT}`);
                console.log(`[SERVER] Port: ${PORT}`);
                console.log("=================================");
            }
        );
    } catch (error) {
        console.error(
            `[SERVER] Startup failed: ${error.message}`
        );
        process.exitCode = 1;
    }
}

startServer();