package com.here.lastmile.fleet.client

import com.here.lastmile.service.config.Config
import java.net.URI
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object AwsDynamoDbClient {

    private lateinit var dynamoDbEnhancedAsyncClient: DynamoDbEnhancedAsyncClient

    /**
     * With guice we would simply inject config and move it to init block.
     */
    fun initialize(config: Config) {
        require(!this::dynamoDbEnhancedAsyncClient.isInitialized) { "client already initialized" }
        dynamoDbEnhancedAsyncClient = DynamoDbEnhancedAsyncClient
            .builder()
            .dynamoDbClient(
                dynamoAsyncClient(config)
            ).build()
    }

    fun getClient(): DynamoDbEnhancedAsyncClient {
        require(this::dynamoDbEnhancedAsyncClient.isInitialized) { "client not initialized." }
        return dynamoDbEnhancedAsyncClient
    }

    private fun dynamoAsyncClient(config: Config): DynamoDbAsyncClient {
        return DynamoDbAsyncClient
            .builder()
            .region(Region.of(config.value("aws.region")))
            .credentialsProvider(createCredentialsProvider(config))
            .endpointOverride(URI.create(config.value("aws.dynamodb.url")))
            .build()
    }

    private fun createCredentialsProvider(config: Config): AwsCredentialsProvider {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                config.value("aws.credentials.key"),
                config.value("aws.credentials.secret")
            )
        )
    }
}
