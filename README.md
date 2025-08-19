# aws-data-processing-mcp-server-demo

AWS CDK + Athena + AWS Data Processing MCP Server を使用したIoTセンサーデータ分析デモ

* [AWS Data Processing MCP Server](https://awslabs.github.io/mcp/servers/aws-dataprocessing-mcp-server/)

## セットアップ

### テストデータの準備

```bash
# 依存関係インストール
$ pnpm install

# CDKブートストラップ（初回のみ）
$ cd packages/iac && npx cdk bootstrap

# インフラデプロイ(クレデンシャルが表示されるので注意!)
$ pnpm run deploy

# データ生成
$ pnpm generate-data

# S3アップロード
$ export S3_BUCKET_NAME=iot-sensor-data-<account>-<region>
$ aws s3 sync packages/sensor-data-generator/data/ s3://$S3_BUCKET_NAME/sensor-data/ --delete
```

### MCPの設定

プロファイルを設定する
```
$ cat ~/.aws/credentials

[iot-demo]
aws_access_key_id = ***
aws_secret_access_key = ***
```

commandには`which uvx`を入れる

```json
{
   "mcpServers": {
      "aws.dp-mcp": {
         "args": [
            "awslabs.aws-dataprocessing-mcp-server@latest",
            "--allow-write",
            "--allow-sensitive-data-access"
         ],
         "command": "/Users/shuntaka/.local/share/mise/installs/python/3.13.5/bin/uvx",
         "env": {
            "AWS_PROFILE": "iot-demo",
            "AWS_REGION": "ap-northeast-1"
         }
      }
   }
}
```

## 削除

```bash
pnpm --filter iac destroy
```
