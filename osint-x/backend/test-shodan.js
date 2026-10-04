const getShodanHost =
    require("./services/shodan");

async function test() {

    const ip = "190.115.16.17";

    console.log("==============================");
    console.log("SHODAN INTERNETDB TEST");
    console.log("==============================");

    try {

        const result =
            await getShodanHost(ip);

        console.log("\nFINAL SHODAN RESULT");
        console.log("==============================");

        console.dir(
            result,
            {
                depth: null
            }
        );

    } catch (error) {

        console.error(
            "\nShodan test failed:"
        );

        console.error(
            error.message
        );
    }
}

test();