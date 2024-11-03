// Lambda function to start the services and re-route the traffic to Fargate services

import {ECSClient, UpdateServiceCommand} from "@aws-sdk/client-ecs";
import {ElasticLoadBalancingV2Client, SetRulePrioritiesCommand} from "@aws-sdk/client-elastic-load-balancing-v2";
import {DescribeDBInstancesCommand, RDSClient, StartDBInstanceCommand} from "@aws-sdk/client-rds";
import {CreateNatGatewayCommand, DescribeNatGatewaysCommand, EC2Client, ReplaceRouteCommand} from "@aws-sdk/client-ec2";

const AWS_REGION = process.env.AWS_REGION;
const SITE_ALB_ARN = process.env.SITE_ALB_ARN;
const UNDER_MAINTENANCE_ALB_ARN = process.env.UNDER_MAINTENANCE_ALB_ARN;

const ecsClient = new ECSClient({region: AWS_REGION});
const elbv2Client = new ElasticLoadBalancingV2Client({region: AWS_REGION});
const rdsClient = new RDSClient({region: AWS_REGION});
const ec2Client = new EC2Client({region: AWS_REGION});

const albRulesHandler = async () => {
    console.log("Re-prioritizing the ALB rules to route the traffic to Fargate service...")
    try {
        const modifiedFixedHtmlRuleResponse = await elbv2Client.send(new SetRulePrioritiesCommand({
            RulePriorities: [
                {
                    Priority: 1,
                    RuleArn: SITE_ALB_ARN
                },
                {
                    Priority: 10,
                    RuleArn: UNDER_MAINTENANCE_ALB_ARN
                }
            ]
        }));
        console.log('Modified ALB rules and routes the traffic tp static Under Maintenance HTML page. Response:', modifiedFixedHtmlRuleResponse);
    } catch (error) {
        console.error('Error while re-prioritizing the rules', error);
        throw error;
    }
}

const startFargateServices = async () => {
    console.log("Starting the Fargate services...")
    const clusterName = process.env.FARGATE_CLUSTER_NAME;
    const webUIServiceName = process.env.WEB_UI_SERVICE_NAME;
    const restApiServiceName = process.env.REST_API_SERVICE_NAME;

    const restApiFargateServiceResponse = await ecsClient.send(new UpdateServiceCommand({
        cluster: clusterName,
        service: restApiServiceName,
        desiredCount: 1
    }));
    console.log(`Started the Rest Api Fargate service ${restApiServiceName}. Response:`, restApiFargateServiceResponse);

    const webUiFargateServiceResponse = await ecsClient.send(new UpdateServiceCommand({
        cluster: clusterName,
        service: webUIServiceName,
        desiredCount: 1
    }));
    console.log(`Started the Web UI Fargate service ${webUIServiceName}. Response:`, webUiFargateServiceResponse);
}

const startDatabaseService = async () => {
    console.log("Starting the Database service...")
    const dbInstanceIdentifier = process.env.RDS_DB_IDENTIFIER;
    const dbInstanceResponse = await rdsClient.send(new StartDBInstanceCommand({
        DBInstanceIdentifier: dbInstanceIdentifier
    }));
    let dbInstanceStatus = dbInstanceResponse.DBInstance.DBInstanceStatus;
    while (dbInstanceStatus !== 'available') {
        console.log(`Waiting for the Database service to become available. Current status: ${dbInstanceStatus}`);
        await new Promise(resolve => setTimeout(resolve, 30000));
        const describeResponse = await rdsClient.send(new DescribeDBInstancesCommand({
            DBInstanceIdentifier: dbInstanceIdentifier
        }));
        dbInstanceStatus = describeResponse.DBInstances[0].DBInstanceStatus;
    }
    console.log('Started the RDS service');
}

const createNatGateway = async () => {
    console.log("Creating the NAT gateway...");
    const createResult = await ec2Client.send(new CreateNatGatewayCommand({
        SubnetId: process.env.NAT_SUBNET_ID,
        AllocationId: process.env.NAT_ALLOCATION_ID,
        TagSpecifications: [
            {
                ResourceType: "natgateway",
                Tags: [
                    {
                        Key: "Name",
                        Value: process.env.NEW_NAT_GATEWAY_NAME
                    }
                ]
            }
        ]
    }));
    let natGatewayId = createResult.NatGateway.NatGatewayId;
    let gatewayState = createResult.NatGateway.State;
    while (gatewayState !== 'available') {
        console.log(`Waiting for the NAT gateway to become available. Current status: ${gatewayState}`);
        await new Promise(resolve => setTimeout(resolve, 30000));
        const describeResult = await ec2Client.send(new DescribeNatGatewaysCommand({
            NatGatewayIds: [natGatewayId]
        }));
        gatewayState = describeResult.NatGateways[0].State;
    }

    //create entry in route table
    await ec2Client.send(new ReplaceRouteCommand({
        RouteTableId: process.env.ROUTE_TABLE_ID,
        DestinationCidrBlock: "0.0.0.0/0",
        NatGatewayId: natGatewayId
    }));

    console.log(`NAT Gateway ${natGatewayId} created successfully.`);
}

export const handler = async (event) => {
    try {
        //stop database service
        await startDatabaseService();

        //start the Fargate services
        await startFargateServices();

        //re-routing traffic to Fargate service
        await albRulesHandler();

        //create NAT gateway
        await createNatGateway();

        return {statusCode: 200, body: 'Services started and ALB rules updated.'};
    } catch (error) {
        console.error('Error starting the services or modifying ALB rules:', error);
        throw error;
    }
};


// zip -r aws-sdk-layer.zip nodejs
