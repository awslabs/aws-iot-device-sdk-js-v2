/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

import { io } from "aws-iot-device-sdk-v2";
import { GGMqtt5Client } from "./gg-mqtt5-client";
import { mqtt5 } from "aws-crt";
import { toUtf8 } from "@aws-sdk/util-utf8-browser";
import { command } from "yargs";

type Args = { [index: string]: any };

// The relative path is '../../util/cli_args' from here, but the compiled javascript file gets put one level
// deeper inside the 'dist' folder
const common_args = require("../../../util/cli_args");

// yargs.command
command(
  "*",
  false,
  (yargs: any) => {
    common_args.add_universal_arguments(yargs);
    common_args.add_topic_message_arguments(yargs);

    yargs
      .option("ca_file", {
        alias: "r",
        description:
          "<path>: path to a Root CA certificate file in PEM format (optional, system trust store used by default).",
        type: "string",
        required: true,
      })
      .option("cert", {
        alias: "c",
        description:
          "<path>: path to a PEM encoded certificate to use with mTLS.",
        type: "string",
        required: true,
      })
      .option("key", {
        alias: "k",
        description:
          "<path>: Path to a PEM encoded private key that matches cert.",
        type: "string",
        required: true,
      })
      .option("thing_name", {
        alias: "n",
        description: "Targeted Thing name.",
        type: "string",
        required: true,
      })
      .option("region", {
        description: "AWS Region.",
        type: "string",
        required: true,
      });
  },
  main
).parse();

async function main(argv: Args) {
  if (argv.verbose && argv.verbose != "none") {
    const level: io.LogLevel = parseInt(
      io.LogLevel[argv.verbose.toUpperCase()]
    );
    io.enable_logging(level);
  }

  const ggMqtt5Client = new GGMqtt5Client({
    rootCAPath: argv.ca_file,
    certificatePath: argv.cert,
    privateKeyPath: argv.key,

    region: argv.region,
    thingName: argv.thing_name,
  });
  try {
    const client = await ggMqtt5Client.connectWithDiscovery();
    console.info("mqtt5Client connect with discovery success");

    if (argv.topic) {
      // example subscribe
      client.subscribe({
        subscriptions: [
          {
            qos: mqtt5.QoS.AtMostOnce,
            topicFilter: argv.topic,
          },
        ],
      });

      // example message received
      client.on(
        "messageReceived",
        async (eventData: mqtt5.MessageReceivedEvent) => {
          console.debug("Message received", { eventData });

          const payloadStr = toUtf8(
            new Uint8Array(eventData.message.payload as ArrayBuffer)
          );
          console.debug("payloadStr", { payloadStr });

          let payload: any;
          try {
            payload = JSON.parse(payloadStr);
          } catch (err) {
            payload = payloadStr;
          }

          console.debug("payload", { payload });

          const responsePayload = { ok: true };

          if (argv.message) {
            // example publish
            console.debug(`Publishing response`);
            await ggMqtt5Client.publish(
              `${eventData.message.topicName}/response`,
              responsePayload
            );
          }
        }
      );
    }
  } catch (err) {
    console.error("Error while connecting MQTT5 client", { err });
  }
}
