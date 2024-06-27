/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


import {iot, mqtt5, mqtt as mqtt311, mqtt_request_response} from "aws-crt";
import {v4 as uuid} from "uuid";
import {once} from "events";
import {IotJobsClientv2} from "./iotjobsclientv2";
//import * as model from "./model";

jest.setTimeout(1000000);

function hasTestEnvironment() : boolean {
    if (process.env.AWS_TEST_MQTT5_IOT_CORE_HOST === undefined) {
        return false;
    }

    if (process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_CERT === undefined) {
        return false;
    }

    if (process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_KEY === undefined) {
        return false;
    }

    return true;
}

const conditional_test = (condition : boolean) => condition ? it : it.skip;

function build_protocol_client_mqtt5() : mqtt5.Mqtt5Client {
    let builder = iot.AwsIotMqtt5ClientConfigBuilder.newDirectMqttBuilderWithMtlsFromPath(
        // @ts-ignore
        process.env.AWS_TEST_MQTT5_IOT_CORE_HOST,
        process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_CERT,
        process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_KEY
    );

    builder.withConnectProperties({
        clientId : uuid(),
        keepAliveIntervalSeconds: 1200,
    });

    return new mqtt5.Mqtt5Client(builder.build());
}

function build_protocol_client_mqtt311() : mqtt311.MqttClientConnection {
    // @ts-ignore
    let builder = iot.AwsIotMqttConnectionConfigBuilder.new_mtls_builder_from_path(process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_CERT, process.env.AWS_TEST_MQTT5_IOT_CORE_RSA_KEY);
    // @ts-ignore
    builder.with_endpoint(process.env.AWS_TEST_MQTT5_IOT_CORE_HOST);
    builder.with_client_id(uuid());

    let client = new mqtt311.MqttClient();
    return client.new_connection(builder.build());
}

enum ProtocolVersion {
    Mqtt311,
    Mqtt5
}

interface TestingOptions {
    version: ProtocolVersion,
    timeoutSeconds?: number,
}

class JobsTestingContext {

    mqtt311Client?: mqtt311.MqttClientConnection;
    mqtt5Client?: mqtt5.Mqtt5Client;

    client: IotJobsClientv2;

    private protocolStarted : boolean = false;

    async startProtocolClient() {
        if (!this.protocolStarted) {
            this.protocolStarted = true;
            if (this.mqtt5Client) {
                let connected = once(this.mqtt5Client, mqtt5.Mqtt5Client.CONNECTION_SUCCESS);
                this.mqtt5Client.start();

                await connected;
            }

            if (this.mqtt311Client) {
                await this.mqtt311Client.connect();
            }
        }
    }

    async stopProtocolClient() {
        if (this.protocolStarted) {
            this.protocolStarted = false;
            if (this.mqtt5Client) {
                let stopped = once(this.mqtt5Client, mqtt5.Mqtt5Client.STOPPED);
                this.mqtt5Client.stop();
                await stopped;

                this.mqtt5Client.close();
            }

            if (this.mqtt311Client) {
                await this.mqtt311Client.disconnect();
            }
        }
    }

    constructor(options: TestingOptions) {
        if (options.version == ProtocolVersion.Mqtt5) {
            this.mqtt5Client = build_protocol_client_mqtt5();

            let rrOptions : mqtt_request_response.RequestResponseClientOptions = {
                maxRequestResponseSubscriptions : 6,
                maxStreamingSubscriptions : 2,
                operationTimeoutInSeconds : options.timeoutSeconds ?? 60,
            }

            this.client = IotJobsClientv2.newFromMqtt5(this.mqtt5Client, rrOptions);
        } else {
            this.mqtt311Client = build_protocol_client_mqtt311();

            let rrOptions : mqtt_request_response.RequestResponseClientOptions = {
                maxRequestResponseSubscriptions : 6,
                maxStreamingSubscriptions : 2,
                operationTimeoutInSeconds : options.timeoutSeconds ?? 60,
            }

            this.client = IotJobsClientv2.newFromMqtt311(this.mqtt311Client, rrOptions);
        }
    }

    async open() {
        await this.startProtocolClient();
    }

    async close() {
        this.client.close();
        await this.stopProtocolClient();
    }
}

async function doCreateDestroyTest(version: ProtocolVersion) {
    let context = new JobsTestingContext({
        version: version
    });
    await context.open();

    await context.close();
}

conditional_test(hasTestEnvironment())('jobsv2 - create destroy mqtt5', async () => {
    await doCreateDestroyTest(ProtocolVersion.Mqtt5);
});

conditional_test(hasTestEnvironment())('jobsv2 - create destroy mqtt311', async () => {
    await doCreateDestroyTest(ProtocolVersion.Mqtt311);
});

interface TestResources {
    thingGroupName?: string,
    thingGroupArn?: string,
    thingName?: string,

    jobId1?: string,
    jobId2?: string,
}

import {
    AddThingToThingGroupCommand,
    CreateJobCommand,
    CreateThingCommand,
    CreateThingGroupCommand,
    DeleteJobCommand,
    DeleteThingCommand,
    DeleteThingGroupCommand,
    IoTClient
} from "@aws-sdk/client-iot";
import * as model from "./model";

//@ts-ignore
let jobResources : TestResources = {};

async function createJob(client : IoTClient, index: number) : Promise<string> {
    let jobId = 'jobid-' + uuid();
    let jobDocument = {
        test: `do-something${index}`
    };

    const createJobCommand = new CreateJobCommand({
        jobId: jobId,
        targets: [ jobResources.thingGroupArn ?? "" ],
        document: JSON.stringify(jobDocument),
        targetSelection: "CONTINUOUS"
    });

    await client.send(createJobCommand);

    return jobId;
}

async function deleteJob(client: IoTClient, jobId: string | undefined) : Promise<void> {
    if (jobId) {
        const command = new DeleteJobCommand({
            jobId: jobId,
            force: true
        });

        await client.send(command);
    }
}

beforeAll(async () => {
    const client = new IoTClient({});

    let thingGroupName = 'tgn-' + uuid();

    const createThingGroupCommand = new CreateThingGroupCommand({
        thingGroupName: thingGroupName
    });

    const createThingGroupResponse = await client.send(createThingGroupCommand);
    jobResources.thingGroupName = thingGroupName;
    jobResources.thingGroupArn = createThingGroupResponse.thingGroupArn;

    let thingName = 't-' + uuid();
    const createThingCommand = new CreateThingCommand({
        thingName: thingName
    });

    await client.send(createThingCommand);
    jobResources.thingName = thingName;

    await new Promise(r => setTimeout(r, 1000));

    jobResources.jobId1 = await createJob(client, 1);

    await new Promise(r => setTimeout(r, 1000));
});

afterAll(async () => {
    const client = new IoTClient({});

    await new Promise(r => setTimeout(r, 1000));

    await deleteJob(client, jobResources.jobId1);
    await deleteJob(client, jobResources.jobId2);

    await new Promise(r => setTimeout(r, 1000));

    if (jobResources.thingName) {
        const command = new DeleteThingCommand({
            thingName: jobResources.thingName
        });

        await client.send(command);

        await new Promise(r => setTimeout(r, 1000));
    }

    if (jobResources.thingGroupName) {
        const command = new DeleteThingGroupCommand({
            thingGroupName: jobResources.thingGroupName
        });

        await client.send(command);
    }
});

async function verifyNoJobExecutions(context: JobsTestingContext) {
    let response = await context.client.getPendingJobExecutions({
        thingName: jobResources.thingName ?? ""
    });
    // @ts-ignore
    expect(response.inProgressJobs.length).toEqual(0);
    // @ts-ignore
    expect(response.queuedJobs.length).toEqual(0);
}

async function attachThingToThingGroup(client: IoTClient) {

    const addThingToThingGroupCommand = new AddThingToThingGroupCommand({
        thingName: jobResources.thingName,
        thingGroupName: jobResources.thingGroupName
    });

    await client.send(addThingToThingGroupCommand);
}

async function doProcessingTest(version: ProtocolVersion) {
    const client = new IoTClient({});

    let context = new JobsTestingContext({
        version: version
    });
    await context.open();

    let jobExecutionChangedStream = context.client.createJobExecutionsChangedStream({
       thingName: jobResources.thingName ?? ""
    });
    jobExecutionChangedStream.on('incomingPublish', (event) => {
        console.log(JSON.stringify(event));
    })
    jobExecutionChangedStream.open();

    let initialExecutionChangedWaiter = once(jobExecutionChangedStream, 'incomingPublish');

    let nextJobExecutionChangedStream = context.client.createNextJobExecutionChangedStream({
       thingName: jobResources.thingName ?? ""
    });
    nextJobExecutionChangedStream.on('incomingPublish', (event) => {
        console.log(JSON.stringify(event));
    })
    nextJobExecutionChangedStream.open();

    //let initialNextJobExecutionChangedWaiter = once(nextJobExecutionChangedStream, 'incomingPublish');

    await verifyNoJobExecutions(context);
    await attachThingToThingGroup(client);

    let initialJobExecutionChanged : model.JobExecutionsChangedEvent = (await initialExecutionChangedWaiter)[0].message;
    // @ts-ignore
    expect(initialJobExecutionChanged.jobs['QUEUED'].length).toEqual(1);
    // @ts-ignore
    expect(initialJobExecutionChanged.jobs['QUEUED'][0].jobId).toEqual(jobResources.jobId1);

    //jobResources.jobId2 = await createJob(client, 2);

    /*
    let testResponse = await context.client.startNextPendingJobExecution({
        thingName: jobResources.thingName ?? ""
    });
    console.log(JSON.stringify(testResponse));
*/
    await new Promise(r => setTimeout(r, 10000));

    let response = await context.client.getPendingJobExecutions({
        thingName: jobResources.thingName ?? ""
    });
    console.log(JSON.stringify(response));

    await context.close();
}

test('jobsv2 processing mqtt5', async () => {
    await doProcessingTest(ProtocolVersion.Mqtt5);
});

conditional_test(hasTestEnvironment())('jobsv2 processing mqtt311', async () => {
    await doProcessingTest(ProtocolVersion.Mqtt311);
});