/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

import { ICrtError } from "aws-crt";
import { greengrass, io, iot, mqtt5 } from "aws-iot-device-sdk-v2";

export interface GGMqtt5ClientOptions {
  readonly rootCAPath: string;
  readonly certificatePath: string;
  readonly privateKeyPath: string;

  readonly region: string;
  readonly thingName: string;
}

/**
 * Represents an MQTT5 Client with helper functions.
 */
export class GGMqtt5Client {
  /**
   * The actual client
   */
  private _mqttClient?: mqtt5.Mqtt5Client;

  /**
   * Passed parameters.
   */
  private options: GGMqtt5ClientOptions;

  /**
   * Instantiates an MQTT5 client.
   * @param options The parameters needed to establish a connection with discovery.
   */
  constructor(options: GGMqtt5ClientOptions) {
    this.options = options;

    console.debug("GGMqtt5Client instantiated with options", options);
  }

  /**
   * Returns whether the client is connected (ie. exists).
   */
  public get connected() {
    return this._mqttClient != null;
  }

  /**
   * Returns the MQTT client object.
   */
  public get client() {
    if (this._mqttClient == null) {
      throw new Error("MQTT client not connected yet.");
    }
    return this._mqttClient;
  }

  /**
   * Connects the MQTT5 client using discovery.
   * @returns The Mqtt5Client promise.
   */
  public async connectWithDiscovery(): Promise<mqtt5.Mqtt5Client> {
    console.debug(`MQTT client connecting...`);
    if (this._mqttClient != null) {
      console.debug(
        `Mqtt client already connected. Returning current active client`
      );
      return Promise.resolve(this.client);
    }

    const clientBootstrap = new io.ClientBootstrap();
    const socketOptions = new io.SocketOptions(
      io.SocketType.STREAM, // type
      io.SocketDomain.IPV4, // domain
      3000 // connect_timeout_ms
    );
    const tlsOptions = new io.TlsContextOptions();
    tlsOptions.override_default_trust_store_from_path(
      undefined,
      this.options.rootCAPath
    );
    tlsOptions.certificate_filepath = this.options.certificatePath;
    tlsOptions.private_key_filepath = this.options.privateKeyPath;

    if (io.is_alpn_available()) {
      console.debug("ALPN available. Adding 'x-amzn-http-ca' to alpn_list", {
        alpnAvailable: io.is_alpn_available(),
      });
      tlsOptions.alpn_list.push("x-amzn-http-ca");
    }

    const tlsContext = new io.ClientTlsContext(tlsOptions);
    const discoveryClient = new greengrass.DiscoveryClient(
      clientBootstrap,
      socketOptions,
      tlsContext,
      this.options.region
    );

    console.debug(`Running discovery`, {
      thingName: this.options.thingName,
    });
    const discoveryResponse = await discoveryClient.discover(
      this.options.thingName
    );
    console.info(`Discovery response`, { discoveryResponse });

    // returns the first connected client;
    // otherwise, if all host_address attempts fail, will return a reject (error)
    return Promise.any(
      discoveryResponse.gg_groups.flatMap((ggGroup) => {
        return ggGroup.cores.flatMap((ggCore) => {
          return ggCore.connectivity.flatMap((connectivity) => {
            console.debug(
              `Connecting to ${connectivity.host_address}:${connectivity.port}`
            );

            // setup the client config
            const mqtt5ClientConfig =
              iot.AwsIotMqtt5ClientConfigBuilder.newDirectMqttBuilderWithMtlsFromPath(
                // take the host address from discovery
                connectivity.host_address,

                // cert and private keys of the client
                this.options.certificatePath,
                this.options.privateKeyPath
              )
                // take the CA from discovery result
                .withCertificateAuthority(ggGroup.certificate_authorities[0])
                // set the session behavior
                .withSessionBehavior(mqtt5.ClientSessionBehavior.RejoinAlways)
                // use socket settings
                .withSocketOptions(
                  new io.SocketOptions(
                    io.SocketType.STREAM,
                    io.SocketDomain.IPV4,
                    3000
                  )
                )
                // set the client ID (current client's thingName)
                .withConnectProperties({
                  clientId: this.options.thingName,
                  keepAliveIntervalSeconds: 60,
                })
                // set the port to connect to
                .withPort(connectivity.port)
                .build();

            // instantiate the client
            const mqttClient = new mqtt5.Mqtt5Client(mqtt5ClientConfig);

            return new Promise<mqtt5.Mqtt5Client>((resolve, reject) => {
              // sign up for the events
              mqttClient.on(
                "attemptingConnect",
                (eventData: mqtt5.AttemptingConnectEvent) => {
                  console.info(
                    `attemptingConnect event: ${connectivity.host_address}:${connectivity.port}`
                  );
                }
              );
              mqttClient.on("error", (error: ICrtError) => {
                console.error("MQTTclient error", { error });
              });
              mqttClient.on(
                "disconnection",
                (eventData: mqtt5.DisconnectionEvent) => {
                  console.info("disconnection event", { eventData });
                }
              );
              mqttClient.on("stopped", (eventData: mqtt5.StoppedEvent) => {
                console.info("stopped event", { eventData });
              });
              mqttClient.on(
                "connectionFailure",
                (eventData: mqtt5.ConnectionFailureEvent) => {
                  console.error(
                    `connectionFailure event: ${connectivity.host_address}:${connectivity.port}`,
                    { eventData }
                  );
                  mqttClient.stop(); // optional todo: try "maxRetry" times
                  reject(eventData);
                }
              );
              mqttClient.on(
                "connectionSuccess",
                (eventData: mqtt5.ConnectionSuccessEvent) => {
                  console.info(
                    `Successfully connected to ${connectivity.host_address}:${connectivity.port}`,
                    { eventData }
                  );
                  if (this.connected) {
                    console.debug(
                      `Already connected. Stopping ${JSON.stringify(
                        mqttClient._handle
                      )}`
                    );
                    mqttClient.stop();
                  } else {
                    // all good, save reference to connected client
                    this._mqttClient = mqttClient;
                    resolve(mqttClient);
                  }
                }
              );

              // start the client
              mqttClient.start();
            });
          });
        });
      })
    );
  }

  /**
   * Publish JSON messages to a topic.
   * @param topic The topic.
   * @param payload The object to publish.
   * @returns The {@link mqtt5.PublishCompletionResult}.
   */
  public async publish(
    topic: string,
    payload: any
  ): Promise<mqtt5.PublishCompletionResult> {
    const publishResp = await this.client.publish({
      topicName: topic,
      qos: mqtt5.QoS.AtLeastOnce,
      payload: JSON.stringify(payload),
    });

    return publishResp;
  }
}
