const { Worker } = require("bullmq");

const {
    connection
} = require("../queue/investigationQueue");

const runSherlock =
    require("../services/sherlock");

const extractDomains =
    require("../utils/domainExtractor");

const resolveDNS =
    require("../services/dns");

const getShodanHost =
    require("../services/shodan");


const disconnectedJobIds = new Set();

let isSocketConnected = () => false;


const worker = new Worker(
    "investigation",

    async (job, token, signal) => {

        console.log("\n==============================");
        console.log("[WORKER] Job started");
        console.log("[JOB ID]:", job.id);
        console.log("[JOB]:", job.name);
        console.log("==============================");


        const {
            target,
            targetType,
            socketId
        } = job.data;


        // =================================
        // CHECK SOCKET
        // =================================

        if (
            !socketId ||
            !isSocketConnected(socketId)
        ) {

            disconnectedJobIds.add(
                String(job.id)
            );

            throw new Error(
                "Client socket is disconnected; removing investigation job"
            );
        }


        console.log("[TARGET]:", target);
        console.log("[TYPE]:", targetType);


        // =================================
        // USERNAME → SHERLOCK
        // =================================

        if (targetType === "username") {

            await job.updateProgress(10);


            console.log(
                "[WORKER] Starting Sherlock..."
            );


            try {

                // =================================
                // SHERLOCK LIVE OUTPUT
                // =================================

                let outputUpdates =
                    Promise.resolve();

                let outputUpdateError;


                const sherlockResult =
                    await runSherlock(
                        target,
                        {
                            signal,

                            onOutput: (output) => {

                                outputUpdates =
                                    outputUpdates.then(() => {

                                        if (
                                            outputUpdateError
                                        ) {
                                            return;
                                        }


                                        return job.updateProgress({
                                            phase: "searching",
                                            output
                                        });

                                    }).catch((error) => {

                                        outputUpdateError =
                                            error;

                                        console.error(
                                            `[WORKER] Failed to publish output for job ${job.id}: ${error.message}`
                                        );

                                    });
                            }
                        }
                    );


                await outputUpdates;


                if (outputUpdateError) {

                    throw outputUpdateError;
                }


                console.log(
                    "[WORKER] Sherlock completed"
                );


                // =================================
                // EXTRACT DOMAINS
                // =================================

                console.log(
                    "[WORKER] Extracting domains..."
                );


                const domains =
                    extractDomains(
                        sherlockResult
                    );


                console.log(
                    "[WORKER] Domains found:",
                    domains
                );


                await job.updateProgress({
                    phase: "domain_extraction",
                    domains
                });


                // =================================
                // DNS RESOLUTION
                // =================================

                console.log(
                    "[WORKER] Starting DNS resolution..."
                );


                const dnsResults =
                    await Promise.all(
                        domains.map(async (domain) => {
                            if (!isSocketConnected(socketId)) {
                                disconnectedJobIds.add(
                                    String(job.id)
                                );

                                throw new Error(
                                    "Client socket disconnected during DNS resolution"
                                );
                            }

                            console.log(
                                `[DNS] Resolving ${domain}`
                            );

                            let dnsResult;

                            try {
                                dnsResult =
                                    await resolveDNS(domain);
                            } catch (error) {
                                console.error(
                                    `[DNS] Failed for ${domain}: ${error.message}`
                                );

                                dnsResult = {
                                    domain,
                                    error: error.message,
                                    ipv4: [],
                                    ipv6: []
                                };
                            }

                            await job.updateProgress({
                                phase: "dns",
                                domain,
                                result: dnsResult
                            });

                            return dnsResult;
                        })
                    );


                // =================================
                // SHODAN INTERNETDB LOOKUPS
                // =================================

                if (!isSocketConnected(socketId)) {
                    disconnectedJobIds.add(
                        String(job.id)
                    );

                    throw new Error(
                        "Client socket disconnected before Shodan lookups"
                    );
                }

                const ipv4Addresses = [
                    ...new Set(
                        dnsResults.flatMap(
                            (dnsResult) => dnsResult.ipv4 || []
                        )
                    )
                ];

                console.log(
                    "[WORKER] Unique IPv4 addresses:",
                    ipv4Addresses
                );

                await job.updateProgress({
                    phase: "shodan_start",
                    ips: ipv4Addresses
                });

                const shodanResults =
                    await Promise.all(
                        ipv4Addresses.map(async (ip) => {
                            if (!isSocketConnected(socketId)) {
                                disconnectedJobIds.add(
                                    String(job.id)
                                );

                                throw new Error(
                                    "Client socket disconnected during Shodan lookups"
                                );
                            }

                            try {
                                const shodanResult =
                                    await getShodanHost(ip);

                                await job.updateProgress({
                                    phase: "shodan",
                                    ip,
                                    result: shodanResult
                                });

                                return shodanResult;
                            } catch (error) {
                                console.error(
                                    `[SHODAN] Failed for ${ip}: ${error.message}`
                                );

                                const shodanError = {
                                    ip,
                                    error: error.message
                                };

                                await job.updateProgress({
                                    phase: "shodan",
                                    ip,
                                    result: shodanError
                                });

                                return shodanError;
                            }
                        })
                    );

                if (!isSocketConnected(socketId)) {
                    disconnectedJobIds.add(
                        String(job.id)
                    );

                    throw new Error(
                        "Client socket disconnected during investigation"
                    );
                }


                // =================================
                // COMPLETED
                // =================================

                await job.updateProgress(100);


                console.log(
                    "[WORKER] DNS resolution completed"
                );


                console.log(
                    "[WORKER] Investigation completed"
                );


                // =================================
                // FINAL RESULT
                // =================================

                return {

                    type: "username",

                    target,

                    sherlock:
                        sherlockResult,

                    domains,

                    dns:
                        dnsResults,

                    shodan:
                        shodanResults

                };


            } catch (error) {

                console.error(
                    "[WORKER] Investigation failed:",
                    error.message
                );

                throw error;
            }
        }


        // =================================
        // OTHER TARGET TYPES
        // =================================

        console.log(
            `[WORKER] No worker implemented for type: ${targetType}`
        );


        throw new Error(
            `No worker implemented for target type: ${targetType}`
        );
    },


    {
        connection,

        autorun: false
    }
);


// =================================
// COMPLETED
// =================================

worker.on(
    "completed",
    (job) => {

        console.log(
            `[WORKER] Job ${job.id} completed`
        );
    }
);


// =================================
// FAILED
// =================================

worker.on(
    "failed",
    (job, error) => {

        console.log(
            `[WORKER] Job ${job?.id} failed`
        );

        console.log(
            `[WORKER] Error: ${error.message}`
        );


        // Remove job that was cancelled
        // because socket disconnected

        if (
            job &&
            disconnectedJobIds.delete(
                String(job.id)
            )
        ) {

            job.remove().then(

                () => {

                    console.log(
                        `[QUEUE] Cancelled job ${job.id} removed after socket disconnect`
                    );
                },

                (removeError) => {

                    console.error(
                        `[QUEUE] Failed to remove cancelled job ${job.id}: ${removeError.message}`
                    );
                }
            );
        }
    }
);


// =================================
// REDIS READY
// =================================

worker.on(
    "ready",
    () => {

        console.log(
            "[WORKER] Connected to Redis and ready for jobs"
        );
    }
);


// =================================
// WORKER ERROR
// =================================

worker.on(
    "error",
    (error) => {

        console.error(
            `[WORKER] Worker error: ${error.message}`
        );
    }
);


// =================================
// CANCEL DISCONNECTED JOB
// =================================

worker.cancelJobForDisconnectedSocket =
    (jobId, reason) => {

        const normalizedJobId =
            String(jobId);


        disconnectedJobIds.add(
            normalizedJobId
        );


        const cancelled =
            worker.cancelJob(
                normalizedJobId,
                reason
            );


        if (!cancelled) {

            disconnectedJobIds.delete(
                normalizedJobId
            );
        }


        return cancelled;
    };


// =================================
// SOCKET CONNECTION CHECKER
// =================================

worker.setSocketIsConnected =
    (checker) => {

        isSocketConnected =
            checker;
    };


module.exports = worker;