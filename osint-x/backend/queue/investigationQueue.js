const { Queue } = require("bullmq");

const connection = {
    host: "127.0.0.1",
    port: 6379
};

const investigationQueue = new Queue(
    "investigation",
    {
        connection
    }
);

module.exports = {
    investigationQueue,
    connection
};