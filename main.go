package main

import (
    "context"
    "encoding/csv"
    "fmt"
    "io"
    "strings"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
    s3Client     *s3.Client
    dynamoClient *dynamodb.Client
    tableName    = "SeuDynamoDBTable"
)

func init() {
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        panic("unable to load SDK config, " + err.Error())
    }

    s3Client = s3.NewFromConfig(cfg)
    dynamoClient = dynamodb.NewFromConfig(cfg)
}

func handler(ctx context.Context, s3Event events.S3Event) error {
    for _, record := range s3Event.Records {
        s3Entity := record.S3
        bucket := s3Entity.Bucket.Name
        key := s3Entity.Object.Key

        // Baixando o arquivo do S3
        resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(key),
        })
        if err != nil {
            return fmt.Errorf("erro ao baixar o arquivo do S3: %v", err)
        }
        defer resp.Body.Close()

        // Processando o arquivo CSV
        reader := csv.NewReader(resp.Body)
        for {
            record, err := reader.Read()
            if err == io.EOF {
                break
            }
            if err != nil {
                return fmt.Errorf("erro ao ler o arquivo CSV: %v", err)
            }

            // Exemplo de dados principais (ajuste conforme necessário)
            id := record[0]
            nome := record[1]
            valor := record[2]

            // Salvando no DynamoDB
            _, err = dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
                TableName: aws.String(tableName),
                Item: map[string]types.AttributeValue{
                    "ID":    &types.AttributeValueMemberS{Value: id},
                    "Nome":  &types.AttributeValueMemberS{Value: nome},
                    "Valor": &types.AttributeValueMemberS{Value: valor},
                },
            })
            if err != nil {
                return fmt.Errorf("erro ao salvar dados no DynamoDB: %v", err)
            }
        }
    }

    return nil
}

func main() {
    lambda.Start(handler)
}
