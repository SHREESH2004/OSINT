const https = require("https");

function getShodanHost(ip) {
    return new Promise((resolve, reject) => {

        console.log(`[SHODAN] InternetDB lookup: ${ip}`);

        const request = https.get(
            `https://internetdb.shodan.io/${ip}`,
            {
                headers: {
                    "User-Agent": "OSINT-X/1.0"
                }
            },
            response => {

                let body = "";

                response.on("data", chunk => {
                    body += chunk;
                });

                response.on("end", () => {

                    if (response.statusCode === 404) {
                        resolve({
                            ip,
                            found: false,
                            ports: [],
                            hostnames: [],
                            cpes: [],
                            tags: [],
                            vulnerabilities: []
                        });

                        return;
                    }

                    if (response.statusCode !== 200) {
                        reject(
                            new Error(
                                `Shodan InternetDB HTTP ${response.statusCode}`
                            )
                        );

                        return;
                    }

                    try {

                        const data =
                            JSON.parse(body);

                        const result = {
                            ip: data.ip || ip,

                            found: true,

                            ports:
                                data.ports || [],

                            hostnames:
                                data.hostnames || [],

                            cpes:
                                data.cpes || [],

                            tags:
                                data.tags || [],

                            vulnerabilities:
                                data.vulns || []
                        };

                        console.log(
                            "[SHODAN] Result:"
                        );

                        console.dir(
                            result,
                            { depth: null }
                        );

                        resolve(result);

                    } catch (error) {

                        reject(
                            new Error(
                                `Invalid Shodan response: ${error.message}`
                            )
                        );
                    }
                });
            }
        );

        request.on("error", error => {
            reject(error);
        });

        request.setTimeout(10000, () => {
            request.destroy();

            reject(
                new Error(
                    "Shodan InternetDB request timed out"
                )
            );
        });
    });
}

module.exports = getShodanHost;