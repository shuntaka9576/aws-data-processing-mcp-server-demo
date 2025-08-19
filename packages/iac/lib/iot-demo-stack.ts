import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import type { Construct } from 'constructs';

export class IoTDemoStack extends cdk.Stack {
  public readonly dataBucket: s3.Bucket;
  public readonly athenaDatabase: glue.CfnDatabase;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for IoT sensor data
    this.dataBucket = new s3.Bucket(this, 'IoTDataBucket', {
      bucketName: `iot-sensor-data-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    // Glue database for Athena
    this.athenaDatabase = new glue.CfnDatabase(this, 'IoTDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'iot_sensor_database',
        description: 'Database for IoT sensor data analysis',
      },
    });

    // Glue table for IoT sensor data with partitions
    new glue.CfnTable(this, 'IoTSensorTable', {
      catalogId: this.account,
      databaseName: this.athenaDatabase.ref,
      tableInput: {
        name: 'sensor_data',
        description: 'IoT sensor data table with date partitions',
        tableType: 'EXTERNAL_TABLE',
        parameters: {
          has_encrypted_data: 'false',
          classification: 'json',
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2030',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `s3://${this.dataBucket.bucketName}/sensor-data/year=\${year}/month=\${month}/day=\${day}/`,
        },
        storageDescriptor: {
          columns: [
            { name: 'device_id', type: 'string' },
            { name: 'timestamp', type: 'string' },
            { name: 'temperature', type: 'double' },
            { name: 'humidity', type: 'double' },
            { name: 'pressure', type: 'double' },
            { name: 'location', type: 'struct<lat:double,lon:double>' },
            { name: 'battery', type: 'int' },
          ],
          location: `s3://${this.dataBucket.bucketName}/sensor-data/`,
          inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
          outputFormat:
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.openx.data.jsonserde.JsonSerDe',
          },
        },
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
      },
    });

    // IAM role for Athena queries
    const athenaRole = new iam.Role(this, 'AthenaQueryRole', {
      assumedBy: new iam.ServicePrincipal('athena.amazonaws.com'),
      inlinePolicies: {
        AthenaPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'athena:*',
                'glue:GetTable',
                'glue:GetDatabase',
                'glue:GetPartitions',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:PutObject',
                's3:DeleteObject',
              ],
              resources: ['*'],
            }),
          ],
        }),
      },
    });

    // Grant Athena access to S3 bucket
    this.dataBucket.grantRead(athenaRole);

    // S3 bucket for Athena query results
    const athenaResultsBucket = new s3.Bucket(this, 'AthenaResultsBucket', {
      bucketName: `athena-query-results-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      versioned: false,
      lifecycleRules: [
        {
          id: 'delete-old-results',
          expiration: cdk.Duration.days(30),
        },
      ],
    });

    // IAM user for Athena queries
    const athenaUser = new iam.User(this, 'AthenaQueryUser', {
      userName: 'athena-query-user',
      managedPolicies: [
        new iam.ManagedPolicy(this, 'AthenaUserPolicy', {
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'athena:Get*',
                'athena:List*',
                'athena:StartQueryExecution',
                'athena:StopQueryExecution',
                'glue:Get*',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:PutObject',
              ],
              resources: ['*'],
            }),
          ],
        }),
      ],
    });

    // Create access key for the Athena user
    const athenaAccessKey = new iam.CfnAccessKey(this, 'AthenaUserAccessKey', {
      userName: athenaUser.userName,
    });

    // Output values
    new cdk.CfnOutput(this, 'DataBucketName', {
      value: this.dataBucket.bucketName,
      description: 'S3 bucket name for IoT sensor data',
    });

    new cdk.CfnOutput(this, 'DatabaseName', {
      value: this.athenaDatabase.ref,
      description: 'Glue database name for Athena queries',
    });

    new cdk.CfnOutput(this, 'AthenaResultsBucketName', {
      value: athenaResultsBucket.bucketName,
      description: 'S3 bucket name for Athena query results',
    });

    new cdk.CfnOutput(this, 'AthenaUserName', {
      value: athenaUser.userName,
      description: 'IAM user name for Athena queries',
    });

    new cdk.CfnOutput(this, 'AthenaAccessKeyId', {
      value: athenaAccessKey.ref,
      description: 'Access key ID for Athena user',
    });

    new cdk.CfnOutput(this, 'AthenaSecretAccessKey', {
      value: athenaAccessKey.attrSecretAccessKey,
      description: 'Secret access key for Athena user (SENSITIVE)',
    });
  }
}
