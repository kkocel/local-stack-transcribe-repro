package com.example.localstacktranscriberepro

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.EnabledService
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import reactor.kotlin.core.publisher.toMono
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import java.time.Duration

@Testcontainers
class AmazonTranscriptionRequesterTest {

    @Container
    val localstack: LocalStackContainer =
        LocalStackContainer(DockerImageName.parse("localstack/localstack:latest-amd64"))
            .withServices(
                Service.S3,
                EnabledService.named("transcribe")
            )

    @Test
    fun `get file from internet and save it to s3`() {
        val s3bucket = "kkocel-localstack-repro"
        val requester = AmazonTranscriptionRequester(
            region = localstack.region,
            credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    localstack.accessKey,
                    localstack.secretKey
                )
            ),
            s3endpointOverride = localstack.getEndpointOverride { "s3" },
            transcriptionEndpointOverride = localstack.getEndpointOverride { "transcript" },
            s3bucket = s3bucket,
            objectMapper = ObjectMapper().findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        )

        requester.s3Client.createBucket(CreateBucketRequest.builder().bucket(s3bucket).build()).toMono().block()

        requester.storeFileOnS3(
            "fileKey"
        )
            .block()

        val name = "job-name-2"
        val trscptRsp =
            requester.startTranscription(
                jobName = name,
                s3Location = "s3://$s3bucket/fileKey",
                outputKey = "outputTranscription"
            ).block()

//        trscptRsp?.transcriptionJob()?.transcriptionJobStatus().shouldBe(TranscriptionJobStatus.IN_PROGRESS)

        await()
            .atMost(Duration.ofMinutes(10))
            .with()
            .pollInterval(Duration.ofSeconds(5))
            .until {
                val output = requester.getTranscriptIfCompleted(name, "outputTranscription")
                    .block()
                println("output: $output")
                output?.isNotEmpty() ?: false
            }
    }
}
