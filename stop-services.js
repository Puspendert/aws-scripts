//Lambda Fn to stop the used resources: Fargate services, RDS, NAT Gateway and route the traffic to a maintenance page

import {ECSClient, UpdateServiceCommand} from "@aws-sdk/client-ecs";
import {ElasticLoadBalancingV2Client, SetRulePrioritiesCommand} from "@aws-sdk/client-elastic-load-balancing-v2";
import {RDSClient, StopDBInstanceCommand} from "@aws-sdk/client-rds";
import {DeleteNatGatewayCommand, DescribeNatGatewaysCommand, EC2Client} from "@aws-sdk/client-ec2";

const AWS_REGION = process.env.AWS_REGION;
const SITE_ALB_ARN = process.env.SITE_ALB_ARN;
const UNDER_MAINTENANCE_ALB_ARN = process.env.UNDER_MAINTENANCE_ALB_ARN;

const ecsClient = new ECSClient({region: AWS_REGION});
const elbv2Client = new ElasticLoadBalancingV2Client({region: AWS_REGION});
const rdsClient = new RDSClient({region: AWS_REGION});
const ec2Client = new EC2Client({region: AWS_REGION});

const albRulesHandler = async () => {
    console.log("Re-prioritizing the ALB rules to route the traffic to static Under Maintenance HTML page...")
    try {
        const modifiedFixedHtmlRuleResponse = await elbv2Client.send(new SetRulePrioritiesCommand({
            RulePriorities: [
                {
                    Priority: 10,
                    RuleArn: SITE_ALB_ARN
                },
                {
                    Priority: 1,
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

const stopFargateService = async () => {
    console.log("Stopping the Fargate services...")
    const clusterName = process.env.FARGATE_CLUSTER_NAME;
    const webUIServiceName = process.env.WEB_UI_SERVICE_NAME;
    const restApiServiceName = process.env.REST_API_SERVICE_NAME;

    const webUiFargateServiceResponse = await ecsClient.send(new UpdateServiceCommand({
        cluster: clusterName,
        service: webUIServiceName,
        desiredCount: 0
    }));
    console.log(`Stopped the Web UI Fargate service ${webUIServiceName}. Response:`, webUiFargateServiceResponse);

    const restApiFargateServiceResponse = await ecsClient.send(new UpdateServiceCommand({
        cluster: clusterName,
        service: restApiServiceName,
        desiredCount: 0
    }));
    console.log(`Stopped the Web UI Fargate service ${restApiServiceName}. Response:`, restApiFargateServiceResponse);

}

const stopDatabaseService = async () => {
    console.log("Stopping the Database service...")
    const dbInstanceIdentifier = process.env.RDS_DB_IDENTIFIER;
    const dbInstanceResponse = await rdsClient.send(new StopDBInstanceCommand({
        DBInstanceIdentifier: dbInstanceIdentifier
    }));
    console.log(`Stopped the Database service ${dbInstanceIdentifier}. Response:`, dbInstanceResponse);
}

const deleteNatGateway = async () => {
    console.log("Deleting the NAT Gateway...")
    const NAT_GATEWAY_NAME = process.env.NEW_NAT_GATEWAY_NAME;
    const describeResult = await ec2Client.send(new DescribeNatGatewaysCommand({
        Filter: [
            {
                Name: 'state',
                Values: ["available"]
            }
        ]
    }));
    const natGateway = describeResult.NatGateways.find((nat) => {
        const nameTag = nat.Tags?.find(tag => tag.Key === 'Name' && tag.Value === NAT_GATEWAY_NAME);
        return !!nameTag;
    });
    if (!natGateway) {
        console.log(`No NAT Gateway found with the name ${NAT_GATEWAY_NAME}`);
        return;
    }
    console.log('Found the NAT Gateway:', natGateway)
    const natGatewayId = natGateway?.NatGatewayId;
    const deleteResult = await ec2Client.send(new DeleteNatGatewayCommand({
        NatGatewayId: natGatewayId
    }));
    console.log(`Deletion initiated for NAT Gateway ${natGatewayId}. Response:`, deleteResult);
}

export const handler = async (event) => {
    try {
        console.log('Stopping services...');

        //re-routing traffic to static Under Maintenance HTML page
        await albRulesHandler();

        //stop the Fargate service
        await stopFargateService();

        //stop database service
        await stopDatabaseService();

        //delete NAT gateway
        await deleteNatGateway();


        return {statusCode: 200, body: 'Services stopped and ALB rules updated.'};
    } catch (error) {
        console.error('Error stopping the services or modifying ALB rules:', error);
        throw error;
    }
};
